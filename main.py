# --- no topo do main.py, imports já existentes ---
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional

class RequisicaoJuncao(BaseModel):
    tipo: str
    credito_desejado: float
    entrada_max: Optional[float] = 0.47
    comissao_extra: Optional[float] = None   # <- agora é obrigatório o usuário informar (pode ser 0)
    prefix: Optional[str] = None             # se um dia quiser prefixo

@app.post("/criar-juncao")
def criar_juncao(req: RequisicaoJuncao):
    # 1) Se o usuário não informou, pedimos confirmação antes de calcular
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
        # lazy import do motor
        from planilha_processor import criar_juncao_sob_demanda as _criar
        resultado = _criar(
            tipo=req.tipo,
            credito_desejado=req.credito_desejado,
            entrada_max=req.entrada_max,
            comissao_extra=req.comissao_extra,
            prefix=req.prefix,
            return_private=False  # nunca expor dados sensíveis no front
        )
        return resultado
    except Exception as e:
        return {"erro": str(e)}
