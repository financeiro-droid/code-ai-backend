import os
import io
import json
from typing import Optional, List, Dict, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Envio interno de lead (Slack)
import requests

# Dependências para leitura do GCS (inventário /cartas e /diag)
try:
    import pandas as pd
    from google.cloud import storage
    HAS_GCS_DEPS = True
except Exception:
    HAS_GCS_DEPS = False

app = FastAPI(title="CoDE.AI Backend")

# ---------------- CORS ----------------
origins = os.getenv(
    "ALLOWED_ORIGINS",
    "https://contempladadescomplicada.com.br,https://www.contempladadescomplicada.com.br"
).split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in origins if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------- Toggle simples de manutenção (opcional) --------------
def _is_updating() -> bool:
    return os.getenv("MAINTENANCE_MODE", "off").lower() in ("on", "1", "true")

# -------------- Models --------------
class RequisicaoJuncao(BaseModel):
    tipo: str
    credito_desejado: float
    entrada_max: Optional[float] = 0.47
    # comissão variável DEVE vir do usuário; se None, pedimos confirmação (HTTP 400)
    comissao_extra: Optional[float] = None
    prefix: Optional[str] = None  # opcional, caso um dia use prefixo

class Selecao(BaseModel):
    solution_id: str
    tipo: str
    credito_desejado: float
    comissao_extra: float
    entrada_max: Optional[float] = 0.47
    prefix: Optional[str] = None

class LeadPayload(BaseModel):
    nome: str
    whatsapp: str
    email: str
    cidade_uf: Optional[str] = None
    melhor_horario: Optional[str] = None
    origem: Optional[str] = None
    selecao: Selecao

# -------------- Utilitários GCS locais --------------
def _build_gcs_client():
    creds_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if not creds_json:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_JSON não configurada no Render.")
    creds = json.loads(creds_json)
    return storage.Client.from_service_account_info(creds)

def _read_all_sheets(bucket_name: str, prefix: str = ""):
    if not HAS_GCS_DEPS:
        raise RuntimeError("Dependências ausentes. Instale pandas, openpyxl e google-cloud-storage.")
    client = _build_gcs_client()
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    frames = []
    for b in blobs:
        n = b.name.lower()
        if not (n.endswith(".xlsx") or n.endswith(".xls")):
            continue
        bio = io.BytesIO(b.download_as_bytes())
        df = pd.read_excel(bio)
        df["__fonte_blob__"] = b.name
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)

def _money_to_float(s):
    if s is None:
        return 0.0
    v = str(s).replace("R$", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(v)
    except Exception:
        return 0.0

def _normalizar(df: "pd.DataFrame"):
    if df is None or df.empty:
        return df
    cols = {c.lower(): c for c in df.columns}
    def pick(*names):
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None
    adm = pick("Administradora")
    tipo = pick("Tipo","Destino","Segmento","Bem","Objetivo")
    credito = pick("Crédito","Credito","Valor Crédito","Valor do Crédito","Valor")
    entrada_f = pick("Entrada Fornecedor","Entrada Parceiro","Entrada")
    parcelas = pick("Parcelas")
    venc = pick("Vencimento","Data de Vencimento")

    out = pd.DataFrame()
    out["administradora"] = df[adm] if adm else ""
    out["tipo"] = df[tipo] if tipo else ""
    out["credito"] = df[credito].apply(_money_to_float) if credito else 0
    out["entrada_fornecedor"] = df[entrada_f].apply(_money_to_float) if entrada_f else 0
    out["parcelas_raw"] = df[parcelas] if parcelas else ""
    out["vencimento"] = pd.to_datetime(df[venc], dayfirst=True, errors="coerce") if venc else pd.NaT
    out = out.dropna(subset=["administradora","tipo"]).reset_index(drop=True)
    out["administradora"] = out["administradora"].astype(str).str.strip()
    out["tipo"] = out["tipo"].astype(str).str.strip()
    return out

# -------------- Endpoints base --------------
@app.get("/")
def root():
    return {"status":"ok","service":"CoDE.AI Backend"}

@app.get("/health")
def health():
    return {"ok": True, "service": "CoDE.AI Backend"}

# -------------- INVENTÁRIO: /cartas --------------
@app.get("/cartas")
def get_cartas(prefix: Optional[str] = None):
    """
    Retorna o inventário cru do bucket (sem junção, sem expor fornecedores).
    """
    if _is_updating():
        return {"cartas": [], "info": "Base em atualização. Tente novamente em instantes."}
    try:
        if not HAS_GCS_DEPS:
            return {"cartas": [], "info": "Dependências do GCS ausentes (pandas/google-cloud-storage)."}
        bucket = os.getenv("GCS_BUCKET", "planilhas-codecalc")
        pref = prefix or os.getenv("GCS_PREFIX", "")
        raw = _read_all_sheets(bucket, pref)
        base = _normalizar(raw)
        if base is None or base.empty:
            return {"cartas": [], "info": "Nenhuma planilha encontrada no bucket/prefixo."}

        rows: List[dict] = []
        for _, r in base.iterrows():
            rows.append({
                "administradora": str(r.get("administradora","")),
                "tipo": str(r.get("tipo","")),
                "credito": float(r.get("credito",0) or 0),
                "entrada_fornecedor": float(r.get("entrada_fornecedor",0) or 0),
                "parcelas": str(r.get("parcelas_raw","")),
                "vencimento": ("" if r.get("vencimento") is None or pd.isna(r.get("vencimento"))
                               else r.get("vencimento").strftime("%d/%m/%Y")),
            })
        return {"cartas": rows[:200], "info": f"{len(rows)} registros totais (preview até 200)."}
    except Exception as e:
        return {"erro": str(e)}

# -------------- JUNÇÃO: /criar-juncao --------------
@app.post("/criar-juncao")
def criar_juncao(req: RequisicaoJuncao):
    """
    Usa o motor para montar as junções (mesma administradora/tipo, sem teto de cartas).
    Regras:
      - 5% da plataforma SEMPRE aplicados.
      - comissão extra DEVE vir do usuário (se None, retorna 400 sugerindo 2%).
    """
    if _is_updating():
        return {"erro": "manutencao", "detalhe": "Base em atualização. Tente novamente em instantes."}

    if req.comissao_extra is None:
        return JSONResponse(
            status_code=400,
            content={
                "erro": "COMISSAO_REQUERIDA",
                "mensagem": "Informe o percentual de comissão do consultor (ex.: 0.00 a 0.10). "
                            "Se quiser um conselho de mercado, normalmente 0.02 (2%) funciona bem. "
                            "Deseja aplicar 0.02?",
                "sugestao_percentual": 0.02
            }
        )

    try:
        # lazy import — app s
