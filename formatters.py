# formatters.py
from datetime import datetime
from typing import List, Dict, Optional

EMOJIS = {
    "imóvel": "🏠🏠",
    "imovel": "🏠🏠",
    "auto": "🚗🚗",
    "serviços": "🛠️🛠️",
    "servicos": "🛠️🛠️",
}

def format_brl(value: float) -> str:
    s = f"{value:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def only_ddmm(date_str: str) -> str:
    if not date_str:
        return ""
    date_str = date_str.strip()
    if len(date_str) == 5 and date_str[2] == "/":
        return date_str
    if "/" in date_str:
        parts = date_str.split("/")
        if len(parts) >= 2:
            return f"{parts[0].zfill(2)}/{parts[1].zfill(2)}"
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%d/%m")
    except Exception:
        pass
    return date_str

def format_parcelas(faixas: List[Dict]) -> str:
    linhas = []
    for f in faixas:
        ini = int(f.get("inicio", 1))
        fim = int(f.get("fim", ini))
        val = float(f.get("valor", 0))
        linhas.append(f"{ini} a {fim}: {format_brl(val)}")
    return "\n".join(linhas)

def normalize_tipo(tipo: Optional[str]) -> str:
    if not tipo:
        return ""
    t = tipo.strip().lower()
    if t in ["imovel", "imóvel"]:
        return "Imóvel"
    if t == "auto":
        return "Auto"
    if t in ["servicos", "serviços"]:
        return "Serviços"
    return tipo.capitalize()

def emojis_for_tipo(tipo: Optional[str]) -> str:
    if not tipo:
        return ""
    t = tipo.strip().lower()
    return EMOJIS.get(t, "")

def block_message(option: Dict) -> str:
    admin = option.get("administradora", "").strip()
    tipo = normalize_tipo(option.get("tipo", ""))
    emoji = emojis_for_tipo(tipo)
    credito = format_brl(float(option.get("credito_total", 0.0)))
    entrada = format_brl(float(option.get("entrada_total", 0.0)))
    parcelas = option.get("parcelas", [])
    venc = only_ddmm(str(option.get("vencimento", "")).strip())
    parcelas_txt = format_parcelas(parcelas) if parcelas else ""
    return (
        f"🔵 {admin} {tipo} {emoji}\n\n"
        f"🧾 Crédito: {credito}\n"
        f"💰 Entrada: {entrada}\n"
        f"💸 Parcelas:\n{parcelas_txt}\n\n"
        f"📅 Vencimento: {venc}\n"
        f"⚠ Taxa de cadastro/transferência à consultar\n"
        f"————————————————————"
    )

def join_blocks(options: List[Dict]) -> str:
    blocos = [block_message(opt) for opt in options[:3]]
    final = "\n\n".join(blocos)
    final += "\n\nQual dessas opções mais te interessou?"
    return final
