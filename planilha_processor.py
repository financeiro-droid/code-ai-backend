# planilha_processor.py
import os, io, json, math, hashlib
from typing import Optional, List, Tuple, Dict, Any
import pandas as pd
from google.cloud import storage

# ---------- Infra ----------
def _build_client():
    creds = json.loads(os.getenv("GCP_SERVICE_ACCOUNT_JSON"))
    return storage.Client.from_service_account_info(creds)

def _read_all_sheets(bucket, prefix=""):
    client = _build_client()
    frames = []
    for b in client.list_blobs(bucket, prefix=prefix):
        n = b.name.lower()
        if n.endswith(".xlsx") or n.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(b.download_as_bytes()))
            df["__fonte_blob__"] = b.name
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ---------- Normalização ----------
def _money_to_float(s):
    if s is None: return 0.0
    v = str(s).replace("R$", "").replace(".", "").replace(",", ".").strip()
    try: return float(v)
    except Exception: return 0.0

def _pick(cols_map, *names):
    for n in names:
        if n.lower() in cols_map:
            return cols_map[n.lower()]
    return None

def _uid_row(r: pd.Series) -> str:
    base = "|".join([
        str(r.get("administradora","")).strip(),
        str(r.get("tipo","")).strip(),
        f'{float(r.get("credito",0.0) or 0.0):.2f}',
        str(r.get("parcelas_raw","")).strip(),
        ("" if pd.isna(r.get("vencimento", None)) else r.get("vencimento").strftime("%Y-%m-%d")),
        str(r.get("fornecedor","")).strip(),
        str(r.get("fonte","")).strip(),
    ])
    return hashlib.sha1(base.encode("utf-8")).hexdigest()  # id estável sem expor dados no front

def _normalizar(df: pd.DataFrame):
    if df is None or df.empty: return df
    cols = {c.lower(): c for c in df.columns}

    administradora = _pick(cols, "Administradora")
    tipo          = _pick(cols, "Tipo","Destino","Segmento","Bem","Objetivo")
    credito       = _pick(cols, "Crédito","Credito","Valor Crédito","Valor do Crédito","Valor")
    entrada_f     = _pick(cols, "Entrada Fornecedor","Entrada Parceiro","Entrada")
    parcelas      = _pick(cols, "Parcelas")
    venc          = _pick(cols, "Vencimento","Data de Vencimento")
    fornecedor    = _pick(cols, "Fornecedor","Parceiro")  # interno only

    out = pd.DataFrame()
    out["administradora"]     = df[administradora] if administradora else ""
    out["tipo"]               = df[tipo] if tipo else ""
    out["credito"]            = df[credito].apply(_money_to_float) if credito else 0
    out["entrada_fornecedor"] = df[entrada_f].apply(_money_to_float) if entrada_f else 0
    out["parcelas_raw"]       = df[parcelas] if parcelas else ""
    out["vencimento"]         = pd.to_datetime(df[venc], dayfirst=True, errors="coerce") if venc else pd.NaT
    out["fornecedor"]         = df[fornecedor] if fornecedor else ""
    out["fonte"]              = df.get("__fonte_blob__", "")

    out = out.dropna(subset=["administradora","tipo"]).reset_index(drop=True)
    out["administradora"] = out["administradora"].astype(str).str.strip()
    out["tipo"]           = out["tipo"].astype(str).str.strip()

    # gera uid estável por linha (nunca exposto como dados sensíveis; só hash no front)
    out["uid"] = out.apply(_uid_row, axis=1)
    return out

# ---------- Utils ----------
def _sumario_parcelas(lst: List[str]) -> str:
    items = [p for p in lst if isinstance(p, str) and p.strip()]
    return " | ".join(items[:6]) + (" ..." if len(items) > 6 else "")

def _solution_id(uids: List[str]) -> str:
    joined = "|".join(sorted(uids))
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()

def _build_public_option(subset: pd.DataFrame, taxa_total: float) -> Dict[str, Any]:
    soma = float(subset["credito"].sum())
    entrada = soma * taxa_total
    parcelas = _sumario_parcelas(subset["parcelas_raw"].tolist())
    venc_min = subset["vencimento"].min()
    return {
        "solution_id": _solution_id(subset["uid"].tolist()),
        "administradora": str(subset["administradora"].iloc[0]),
        "tipo": str(subset["tipo"].iloc[0]),
        "credito_total": round(soma, 2),
        "entrada": round(entrada, 2),
        "parcelas": parcelas,
        "vencimento_mais_proximo": ("" if pd.isna(venc_min) else venc_min.strftime("%d/%m/%Y")),
        "cartas_usadas": int(subset.shape[0]),
    }

def _build_private(subset: pd.DataFrame) -> Dict[str, Any]:
    # pacote apenas para backoffice (nunca vai ao front)
    return {
        "uids": subset["uid"].tolist(),
        "fornecedores": [str(x) for x in subset["fornecedor"].tolist()],
        "blobs": [str(x) for x in subset["fonte"].tolist()],
        "creditos_individuais": [float(x) for x in subset["credito"].tolist()],
        "parcelas_individuais": [str(x) for x in subset["parcelas_raw"].tolist()],
        "vencimentos": [
            ("" if pd.isna(x) else x.strftime("%d/%m/%Y")) for x in subset["vencimento"].tolist()
        ],
    }

# ---------- Solvers (minimize sum >= target; sem teto de cartas) ----------
from itertools import combinations
def _mitm_min_cover(values: List[Tuple[float, int]], target: float) -> Tuple[float, List[int]]:
    n = len(values); m = n // 2
    left, right = values[:m], values[m:]

    def all_sums(arr):
        sums = []
        for r in range(len(arr)+1):
            for comb in combinations(arr, r):
                s = sum(v for v, _ in comb)
                idxs = [i for _, i in comb]
                sums.append((s, idxs))
        sums.sort(key=lambda x: x[0])
        return sums

    L, R = all_sums(left), all_sums(right)
    import bisect
    R_sums = [s for s, _ in R]

    best_sum, best_idxs = float("inf"), []
    for sL, iL in L:
        need = target - sL
        if need <= 0:
            if sL < best_sum: best_sum, best_idxs = sL, iL
            continue
        j = bisect.bisect_left(R_sums, need)
        if j < len(R):
            s = sL + R[j][0]
            if s >= target and s < best_sum:
                best_sum, best_idxs = s, iL + R[j][1]
    return best_sum, best_idxs

def _fptas_min_cover(values: List[Tuple[float, int]], target: float, eps: float = None, max_states: int = None) -> Tuple[float, List[int]]:
    eps = eps or float(os.getenv("CODE_FPTAS_EPS", "0.01"))
    max_states = max_states or int(os.getenv("CODE_FPTAS_MAX", "5000"))

    states: List[Tuple[float, List[int]]] = [(0.0, [])]
    for v, idx in sorted(values, key=lambda x: -x[0]):
        added = [(s+v, ids+[idx]) for (s, ids) in states]
        merged = []
        i = j = 0
        states_sorted = states
        added.sort(key=lambda x: x[0])

        while i < len(states_sorted) or j < len(added):
            cand = None
            if j >= len(added) or (i < len(states_sorted) and states_sorted[i][0] <= added[j][0]):
                cand = states_sorted[i]; i += 1
            else:
                cand = added[j]; j += 1
            if not merged or cand[0] > merged[-1][0]:
                merged.append(cand)

        trimmed = []
        last = -1.0
        for s, ids in merged:
            if not trimmed:
                trimmed.append((s, ids)); last = s
            else:
                if s >= last * (1.0 + eps):
                    trimmed.append((s, ids)); last = s

        if len(trimmed) > max_states:
            step = math.ceil(len(trimmed) / max_states)
            trimmed = trimmed[::step]
        states = trimmed

    for s, ids in states:
        if s >= target:
            return s, ids
    return states[-1][0], states[-1][1]

def _min_cover(values: List[Tuple[float, int]], target: float) -> Tuple[float, List[int]]:
    return _mitm_min_cover(values, target) if len(values) <= 26 else _fptas_min_cover(values, target)

# ---------- API ----------
def criar_juncao_sob_demanda(tipo: Optional[str],
                             credito_desejado: float,
                             entrada_max: Optional[float] = 0.47,
                             comissao_extra: Optional[float] = 0.0,
                             prefix: Optional[str] = None,
                             return_private: bool = False) -> Dict[str, Any]:
    """
    Combina cartas (mesma administradora/tipo) sem teto de quantidade.
    NÃO aplica 2% automaticamente; usa a comissão informada pelo usuário.
    Aplica SEMPRE 5% da plataforma.
    """
    taxa_total = 0.05 + float(comissao_extra or 0.0)  # 5% fixo + comissão do usuário (se 0, tudo bem)

    bucket = os.getenv("GCS_BUCKET", "planilhas-codecalc")
    pref = prefix or os.getenv("GCS_PREFIX", "")

    base = _normalizar(_read_all_sheets(bucket, pref))
    if base is None or base.empty:
        return {"opcoes": [], "info": "Sem base de cartas no bucket."}

    df = base.copy()
    if tipo:
        df = df[df["tipo"].str.lower() == str(tipo).lower()]
    if df.empty:
        return {"opcoes": [], "info": "Nenhuma carta compatível com o tipo informado."}

    opcoes: List[Dict[str, Any]] = []
    for (_, _), grp in df.groupby(["administradora", "tipo"], dropna=False):
        values = [(float(v or 0.0), int(i)) for i, v in enumerate(grp["credito"].tolist()) if float(v or 0.0) > 0]
        if not values: continue

        best_sum, idxs_local = _min_cover(values, float(credito_desejado or 0.0))
        subset = grp.iloc[idxs_local]

        entrada = best_sum * taxa_total
        if (entrada_max is not None) and (entrada > best_sum * float(entrada_max)):
            continue

        pub = _build_public_option(subset, taxa_total)
        if return_private:
            pub["private"] = _build_private(subset)
        opcoes.append(pub)

    if not opcoes:
        return {"opcoes": [], "info": "Não foi possível montar junções dentro do teto de entrada."}

    opcoes = sorted(opcoes, key=lambda x: (x["entrada"], x["credito_total"]))[:10]
    return {
        "opcoes": opcoes,
        "politica_comissao": {"base": 0.05, "extra_usada": float(comissao_extra or 0.0), "total": taxa_total}
    }
