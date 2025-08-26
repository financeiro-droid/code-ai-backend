import pandas as pd
from google.cloud import storage
import os

# Carrega credenciais do Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "code-ai-469022-9d7d5cebd46f.json"
BUCKET_NAME = "planilhas-codecalc"

COMISSAO_FIXA = 5  # 5% padrão da Contemplada Descomplicada

def ler_planilhas_bucket():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()
    planilhas = []
    for blob in blobs:
        if blob.name.endswith(".xlsx"):
            blob.download_to_filename(blob.name)
            planilhas.append(blob.name)
    return planilhas

def ler_e_processar_planilha(nome_arquivo):
    df = pd.read_excel(nome_arquivo)
    df["Fornecedor"] = nome_arquivo.split(".")[0]
    return df

def aplicar_regras(df, comissao_usuario=0):
    df = df.copy()
    df["Entrada"] = df["Entrada Fornecedor"] + (df["Crédito"] * ((COMISSAO_FIXA + comissao_usuario) / 100))

    def selecionar_melhores(grupo):
        grupo_ordenado = grupo.sort_values(by=["Entrada", "Parcelas"], ascending=[True, True])
        return grupo_ordenado.head(3).to_dict(orient="records")

    resultados = {
        "Imóvel": selecionar_melhores(df[df["Destino/Tipo"] == "Imóvel"]),
        "Auto": selecionar_melhores(df[df["Destino/Tipo"] == "Auto"]),
        "Serviços": selecionar_melhores(df[df["Destino/Tipo"] == "Serviços"]),
    }
    return resultados

def obter_juncoes(comissao_usuario=0):
    arquivos = ler_planilhas_bucket()
    dfs = [ler_e_processar_planilha(arq) for arq in arquivos]
    if not dfs:
        return {"mensagem": "Nenhuma planilha encontrada."}
    df_total = pd.concat(dfs, ignore_index=True)
    return aplicar_regras(df_total, comissao_usuario)
