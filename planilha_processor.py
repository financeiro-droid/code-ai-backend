# planilha_processor.py
import os, io, json, math
from typing import Optional, List, Tuple, Dict, Any
import pandas as pd
from google.cloud import storage

# =========================
# Infra GCS
# =========================
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

# =========================
# Normalização
# =========================
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

def _normalizar(df: pd.DataFrame):
    if df is None or df.empty: return df
    cols = {c.lower(): c for c in df.columns}

    administradora = _pick(cols, "Administradora")
    tipo          = _pick(cols, "Tipo", "Destino", "Segmento", "Bem", "Objetivo")
    credito       = _pick(cols, "Crédito", "Credito", "Valor Crédito", "Valor do Crédito", "Valor")
    entrada_f     = _pick(cols, "Entrada Fornecedor", "Entrada Parceiro", "Entrada")
    parcelas      = _pick(cols, "Parcelas")
    venc          = _pick(cols, "Vencimento", "Data de Vencimento")
    fornecedor    = _pick(cols, "Fornecedor", "Parceiro")  # só para backoffice (não expor)
    out = pd.DataFrame()
    out["administradora"]     = df[administradora] if administradora else ""
    out["tipo"]               = df[tipo] if tipo else ""
    out["credito"]            = df[credito].apply(_money_to_float) if credito else 0
    out["entrada_fornecedor"] = df[entrada_f].apply(_money_to_float) if entrada_f else 0
    out["parcelas_raw"]       = df[parcelas] if parcelas else ""
    out["vencimento"]         = pd.to_datetime(df[venc], dayfirst=True, errors="coerce") if venc else pd.NaT
    out["fornecedor"]         = df[fornecedor] if fornecedor else ""
    out["fonte"]              = df.get("__fonte_blob__", "")
    out = out.dropna(subset=["administradora", "tipo"]).reset_index(drop=True)
    out["administradora"] = out["administradora"].astype(str).str.strip()
    out["tipo"] = out["tipo"].astype(str).str.strip()
    return out

# =========================
# Utilitários
# =========================
def _sumario_parcelas(parcelas_list: List[str]) -> str:
    itens = [p for p in parcelas_list if isinstance(p, str) and p.strip()]
    return " | ".join(itens[:6]) + (" ..." if len(itens) > 6 else "")

def _build_option(subset: pd.DataFrame, taxa_total: float) -> Dict[str, Any]:
    soma = float(subset["credito"].sum())
    entrada = soma * taxa_total
    parcelas = _sumario_parcelas(subset["parcelas_raw"].tolist())
    venc_min = subset["vencimento"].min()
    return {
        "administradora": str(subset["administradora"].iloc[0]),
        "tipo": str(subset["tipo"].iloc[0]),
        "credito_total": round(soma, 2),
        "entrada": round(entrada, 2),
        "parcelas": parcelas,
        "vencimento_mais_proximo": ("" if pd.isna(venc_min) else venc_min.strftime("%d/%m/%Y")),
        "cartas_usadas": int(subset.shape[0]),  # contagem, sem expor fornecedor
    }

# =========================
# Solvers (sem limite de cartas)
# =========================
def _mitm_min_cover(values: List[Tuple[float, int]], target: float) -> Tuple[float, List[int]]:
    """
    Meet-in-the-middle exato para N até ~26.
    values: [(valor, idx), ...]
    Retorna (soma_minima>=target, indices)
    """
    from itertools import combinations
    n = len(values)
    m = n // 2
    left = values[:m]
    right = values[m:]

    def all_sums(arr):
        sums = []
        for r in range(len(arr)+1):
            for comb in combinations(arr, r):
                s = sum(v for v, _ in comb)
                idxs = [i for _, i in comb]
                sums.append((s, idxs))
        sums.sort(key=lambda x: x[0])
        return sums

    L = all_sums(left)
    R = all_sums(right)
    # para busca eficiente do complemento mínimo
    import bisect
    R_sums = [s for s, _ in R]

    best_sum = float("inf")
    best_idxs: List[int] = []
    for sL, iL in L:
        need = target - sL
        if need <= 0:
            if sL < best_sum:
                best_sum, best_idxs = sL, iL
            continue
        j = bisect.bisect_left(R_sums, need)
        if j < len(R):
            s = sL + R[j][0]
            if s >= target and s < best_sum:
                best_sum = s
                best_idxs = iL + R[j][1]
    return best_sum, best_idxs

def _fptas_min_cover(values: List[Tuple[float, int]], target: float, eps: float = None, max_states: int = None) -> Tuple[float, List[int]]:
    """
    FPTAS aproximado p/ 'min-sum >= target'.
    Mantém lista de estados (sum, idxs) com trimming multiplicativo para controlar tamanho.
    """
    eps = eps or float(os.getenv("CODE_FPTAS_EPS", "0.01"))  # 1% de tolerância por default
    max_states = max_states or int(os.getenv("CODE_FPTAS_MAX", "5000"))

    # estados é lista de (sum, idxs)
    states: List[Tuple[float, List[int]]] = [(0.0, [])]

    for v, idx in values:
        # merge: sem o item e com o item
        merged: List[Tuple[float, List[int]]] = []
        i, j = 0, 0
        added = [(s+v, ids+[idx]) for (s, ids) in states]
        # ambos já ordenados por s (states garantimos ordenado a cada passo)
        states_sorted = states  # já está
        added.sort(key=lambda x: x[0])

        while i < len(states_sorted) or j < len(added):
            cand = None
            if j >= len(added) or (i < len(states_sorted) and states_sorted[i][0] <= added[j][0]):
                cand = states_sorted[i]; i += 1
            else:
                cand = added[j]; j += 1
            if not merged or cand[0] > merged[-1][0]:
                merged.append(cand)

        # trimming multiplicativo
        trimmed: List[Tuple[float, List[int]]] = []
        last = -1.0
        for s, ids in merged:
            if not trimmed:
                trimmed.append((s, ids)); last = s
            else:
                if s >= last * (1.0 + eps):
                    trimmed.append((s, ids)); last = s

        # proteção extra de tamanho
        if len(trimmed) > max_states:
            step = math.ceil(len(trimmed) / max_states)
            trimmed = trimmed[::step]

        states = trimmed

    # pega o primeiro >= target
    for s, ids in states:
        if s >= target:
            return s, ids
    # se nenhum bateu, retorna maior (somou tudo)
    return states[-1][0], states[-1][1]

def _min_cover(values: List[Tuple[float, int]], target: float) -> Tuple[float, List[int]]:
    """
    Escolhe solver conforme tamanho: exato (mitm) p/ N<=26; aproximado p/ N grande.
    """
    if len(values) <= 26:
        return _mitm_min_cover(values, target)
    # ordenar por maior crédito ajuda no trimming convergir mais rápido
    values = sorted(values, key=lambda x: -x[0])
    return _fptas_min_cover(values, target)

# =========================
# API principal
# =========================
def criar_juncao_sob_demanda(tipo: Optional[str],
                             credito_desejado: float,
                             entrada_max: Optional[float] = 0.47,
                             comissao_extra: Optional[float] = 0.0,
                             prefix: Optional[str] = None) -> Dict[str, Any]:
    """
    Combina cartas de fornecedores diferentes, preservando MESMA ADMINISTRADORA e MESMO TIPO.
    Sem limite de quantidade de cartas. Minimiza a soma de crédito (logo, a entrada).
    """
    # Comissão: se o cliente não souber, aplicamos +2% (média mercado)
    if not comissao_extra or comissao_extra < 0:
        comissao_extra = 0.02
    taxa_total = 0.05 + float(comissao_extra or 0.0)

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

    # Agrupar por (administradora, tipo) e resolver min-cover por grupo
    melhores: List[Dict[str, Any]] = []
    for (adm, tp), grp in df.groupby(["administradora", "tipo"], dropna=False):
        values = [(float(v or 0.0), int(i)) for i, v in enumerate(grp["credito"].tolist()) if float(v or 0.0) > 0]
        if not values:
            continue
        best_sum, idxs_local = _min_cover(values, float(credito_desejado or 0.0))
        subset = grp.iloc[idxs_local]

        # Filtrar por entrada_max (mantido por compatibilidade, ainda que taxa_total <= entrada_max costume ser sempre True)
        entrada = best_sum * taxa_total
        if (entrada_max is not None) and (entrada > best_sum * float(entrada_max)):
            # nesse caso, não há solução que respeite o teto de entrada para este grupo
            continue

        melhores.append(_build_option(subset, taxa_total))

    if not melhores:
        return {"opcoes": [], "info": "Não foi possível montar junções dentro do teto de entrada."}

    # Ordena por menor entrada e depois menor crédito_total, e entrega até 10
    melhores = sorted(melhores, key=lambda x: (x["entrada"], x["credito_total"]))[:10]
    return {
        "opcoes": melhores,
        "politica_comissao": {"base": 0.05, "extra_usada": float(comissao_extra), "total": taxa_total},
        "governanca": {"regra": "mesma_administradora_mesmo_tipo", "mix_fornecedores": True, "solver": "mitm|fptas"},
    }
