import os
import io
import json
from typing import Optional, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# --- IMPORTANTE: mantemos sua lógica de junção existente ---
# Usamos sua função atual para o endpoint /criar-juncao.
# (Removemos o uso do argumento inexistente 'apenas_cartas'.)
from planilha_processor import criar_juncao_sob_demanda  # sua função atual

# --- Dependências para ler o bucket (inventário de cartas) ---
# Se faltar alguma, o /cartas e /diag retornam mensagem clara.
try:
    import pandas as pd
    from google.cloud import storage
    HAS_GCS_DEPS = True
except Exception:  # ImportError
    HAS_GCS_DEPS = False

app = FastAPI(title="CoDE.AI Backend")

# -------------------------
# CORS
# -------------------------
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

# -------------------------
# Modelos Pydantic
# -------------------------
class RequisicaoJuncao(BaseModel):
    tipo: str
    credito_desejado: float
    entrada_max: Optional[float] = 0.47
    comissao_extra: Optional[float] = 0.0


# -------------------------
# Utilitários GCS locais (para não depender de outros módulos)
# -------------------------
def _build_gcs_client():
    """Cria o client do GCS a partir da variável GCP_SERVICE_ACCOUNT_JSON."""
    creds_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if not creds_json:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_JSON não configurada no Render.")
    creds = json.loads(creds_json)
    return storage.Client.from_service_account_info(creds)

def _read_all_sheets(bucket_name: str, prefix: str = ""):
    """Lê todas as planilhas .xlsx/.xls do bucket/prefix e concatena em um DataFrame."""
    if not HAS_GCS_DEPS:
        raise RuntimeError("Dependências do GCS/pandas ausentes. Instale google-cloud-storage, pandas e openpyxl.")

    client = _build_gcs_client()
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    frames = []
    for b in blobs:
        name = b.name.lower()
        if not (name.endswith(".xlsx") or name.endswith(".xls")):
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
    v = str(s)
    v = v.replace("R$", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(v)
    except Exception:
        return 0.0

def _normalizar(df: "pd.DataFrame"):
    """Mapeia colunas comuns para um formato padronizado usado no inventário /cartas."""
    if df is None or df.empty:
        return df

    cols = {c.lower(): c for c in df.columns}

    def pick(*names):
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None

    adm = pick("Administradora")
    tipo = pick("Tipo", "Destino", "Segmento", "Bem", "Objetivo")
    credito = pick("Crédito", "Credito", "Valor Crédito", "Valor do Crédito", "Valor")
    entrada_f = pick("Entrada Fornecedor", "Entrada Parceiro", "Entrada")
    parcelas = pick("Parcelas")
    venc = pick("Vencimento", "Data de Vencimento")

    out = pd.DataFrame()
    out["administradora"] = df[adm] if adm else ""
    out["tipo"] = df[tipo] if tipo else ""
    out["credito"] = df[credito].apply(_money_to_float) if credito else 0
    out["entrada_fornecedor"] = df[entrada_f].apply(_money_to_float) if entrada_f else 0
    out["parcelas_raw"] = df[parcelas] if parcelas else ""
    out["vencimento"] = pd.to_datetime(df[venc], dayfirst=True, errors="coerce") if venc else pd.NaT

    # Apenas registros com admin/tipo/crédito válidos
    out = out.dropna(subset=["administradora", "tipo"]).reset_index(drop=True)
    return out


# -------------------------
# Endpoints básicos
# -------------------------
@app.get("/")
def root():
    return {"status": "ok", "service": "CoDE.AI Backend"}

@app.get("/health")
def health():
    return {"ok": True, "service": "CoDE.AI Backend"}


# -------------------------
# INVENTÁRIO: GET /cartas (desacoplado da junção)
# -------------------------
@app.get("/cartas")
def get_cartas(prefix: Optional[str] = None):
    """
    Retorna o INVENTÁRIO cru de cartas do bucket (sem expor fornecedores, sem montar junção).
    """
    try:
        if not HAS_GCS_DEPS:
            retur
