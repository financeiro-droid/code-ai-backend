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
# Libera acesso ao frontend do CoDE no WordPress
origins = [
    "https://contempladadescomplicada.com.br",
    "https://www.contempladadescomplicada.com.br",
    "http://localhost",  # útil para testes locais
    "https://code-ai-backend-rcye.onrender.com"  # libera chamadas entre backend e frontend
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
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
    comissao_extra: Optional[float] = None
    prefix: Optional[str] = None

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
        from planilha_processor import criar_juncao_sob_demanda as _criar
        resultado = _criar(
            tipo=req.tipo,
            credito_desejado=req.credito_desejado,
            entrada_max=req.entrada_max,
            comissao_extra=req.comissao_extra,
            prefix=req.prefix,
            return_private=False
        )
        return resultado
    except Exception as e:
        return {"erro": str(e)}

# -------------- LEAD: /lead --------------
@app.post("/lead")
def receber_lead(payload: LeadPayload):
    admin_webhook = os.getenv("ADMIN_WEBHOOK", "")
    escolhida: Dict[str, Any] = None
    try:
        from planilha_processor import criar_juncao_sob_demanda as _criar
        resultado = _criar(
            tipo=payload.selecao.tipo,
            credito_desejado=payload.selecao.credito_desejado,
            entrada_max=payload.selecao.entrada_max,
            comissao_extra=payload.selecao.comissao_extra,
            prefix=payload.selecao.prefix,
            return_private=True
        )
        opcoes = resultado.get("opcoes", [])
        escolhida = next((o for o in opcoes if o.get("solution_id") == payload.selecao.solution_id), None)
    except Exception:
        escolhida = None

    txt = [
        "*[NOVO LEAD CoDE]*",
        f"*Nome:* {payload.nome}",
        f"*WhatsApp:* {payload.whatsapp}",
        f"*E-mail:* {payload.email}",
        f"*Cidade/UF:* {payload.cidade_uf or '-'}",
        f"*Horário preferido:* {payload.melhor_horario or '-'}",
        f"*Origem:* {payload.origem or '-'}",
        "",
        "*Solicitação:*",
        f"- Tipo: {payload.selecao.tipo}",
        f"- Crédito desejado: R$ {payload.selecao.credito_desejado:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        f"- Comissão variável: {payload.selecao.comissao_extra*100:.2f}% (fixo plataforma: 5%)",
    ]
    if escolhida:
        pol = f"{(0.05+payload.selecao.comissao_extra)*100:.2f}%"
        txt += [
            "",
            "*Junção selecionada:*",
            f"- solution_id: {escolhida.get('solution_id')}",
            f"- Administradora/Tipo: {escolhida.get('administradora')} / {escolhida.get('tipo')}",
            f"- Crédito total: R$ {escolhida.get('credito_total'):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            f"- Entrada (c/ comissão total {pol}): R$ {escolhida.get('entrada'):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            f"- Cartas usadas: {escolhida.get('cartas_usadas')}",
            f"- Parcelas: {escolhida.get('parcelas')}",
        ]
        priv = escolhida.get("private", {})
        fornecedores = priv.get("fornecedores", [])
        blobs = priv.get("blobs", [])
        creditos = priv.get("creditos_individuais", [])
        parcelas = priv.get("parcelas_individuais", [])
        vencs = priv.get("vencimentos", [])
        if creditos:
            txt.append("")
            txt.append("*Cartas (detalhe interno/confidencial):*")
            for i in range(len(creditos)):
                linha = (
                    f"  • Fornecedor: {fornecedores[i] or '-'} | "
                    f"Crédito: R$ {creditos[i]:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    + f" | Venc.: {vencs[i] or '-'} | Fonte: {blobs[i]}"
                )
                if i < len(parcelas) and parcelas[i]:
                    linha += f" | Parcelas: {parcelas[i]}"
                txt.append(linha)

    if admin_webhook:
        try:
            requests.post(admin_webhook, json={"text": "\n".join(txt)}, timeout=10)
        except Exception:
            pass

    return {"ok": True, "message": "Recebemos seus dados. Um consultor CoDE entrará em contato."}

# -------------- DIAGNÓSTICO: /diag --------------
@app.get("/diag")
def diag():
    try:
        if not HAS_GCS_DEPS:
            return {"ok": False, "error": "Dependências ausentes (pandas/google-cloud-storage)."}
        bucket = os.getenv("GCS_BUCKET","planilhas-codecalc")
        pref = os.getenv("GCS_PREFIX","")
        raw = _read_all_sheets(bucket, pref)
        if raw is None or raw.empty:
            return {"ok": True, "bucket": bucket, "prefix": pref, "rows_detected": 0, "columns": []}
        return {
            "ok": True,
            "bucket": bucket,
            "prefix": pref,
            "rows_detected": len(raw),
            "columns": list(raw.columns)[:20],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
