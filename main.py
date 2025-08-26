from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from planilha_processor import criar_juncao_sob_demanda

app = FastAPI()

# Modelo esperado na requisição
class RequisicaoJuncao(BaseModel):
    tipo: str
    credito_desejado: float
    entrada_max: Optional[float] = 0.47
    comissao_extra: Optional[float] = 0.0

@app.get("/cartas")
def get_cartas():
    try:
        resultados = criar_juncao_sob_demanda(tipo=None, credito_desejado=None, apenas_cartas=True)
        return resultados
    except Exception as e:
        return {"erro": str(e)}

@app.post("/criar-juncao")
def criar_juncao(req: RequisicaoJuncao):
    try:
        resultado = criar_juncao_sob_demanda(
            tipo=req.tipo,
            credito_desejado=req.credito_desejado,
            entrada_max=req.entrada_max,
            comissao_extra=req.comissao_extra
        )
        return resultado
    except Exception as e:
        return {"erro": str(e)}
