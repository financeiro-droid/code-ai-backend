import pandas as pd
from google.cloud import storage
import os
from itertools import combinations

BUCKET_NAME = "planilhas-codecalc"
CREDENTIALS_PATH = "service_account_key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

def baixar_planilhas_do_bucket():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()
    arquivos_salvos = []

    for blob in blobs:
        if blob.name.endswith(".xlsx"):
            caminho_local = f"temp_{blob.name.replace('/', '_')}"
            blob.download_to_filename(caminho_local)
            arquivos_salvos.append((caminho_local, blob.name))
    return arquivos_salvos

def processar_planilha():
    arquivos = baixar_planilhas_do_bucket()
    if not arquivos:
        return {"mensagem": "Nenhuma planilha encontrada no bucket."}

    dfs = []
    for caminho, nome in arquivos:
        df = pd.read_excel(caminho)
        df["Fornecedor"] = nome.replace(".xlsx", "")
        dfs.append(df)

    df_total = pd.concat(dfs, ignore_index=True)

    return {
        "Imóvel": df_total[df_total["Destino/Tipo"] == "Imóvel"].head(5).to_dict(orient="records"),
        "Auto": df_total[df_total["Destino/Tipo"] == "Auto"].head(5).to_dict(orient="records"),
        "Serviços": df_total[df_total["Destino/Tipo"] == "Serviços"].head(5).to_dict(orient="records"),
    }

def criar_juncao_sob_demanda(tipo, credito_desejado, entrada_max, comissao_extra=0.0):
    arquivos = baixar_planilhas_do_bucket()
    if not arquivos:
        return []

    dfs = []
    for caminho, nome in arquivos:
        df = pd.read_excel(caminho)
        df["Fornecedor"] = nome.replace(".xlsx", "")
        dfs.append(df)

    df_total = pd.concat(dfs, ignore_index=True)
    df_total = df_total[df_total["Destino/Tipo"] == tipo]

    df_total["Crédito"] = pd.to_numeric(df_total["Crédito"], errors="coerce")
    df_total["Entrada Fornecedor"] = pd.to_numeric(df_total["Entrada Fornecedor"], errors="coerce")
    df_total.dropna(subset=["Crédito", "Entrada Fornecedor"], inplace=True)

    opcoes = []
    for r in range(1, 9):  # até 8 cartas
        for combinacao in combinations(df_total.to_dict(orient="records"), r):
            soma_credito = sum(c["Crédito"] for c in combinacao)
            soma_entrada = sum(c["Entrada Fornecedor"] for c in combinacao)
            comissao_total = soma_credito * (0.05 + comissao_extra)
            entrada_total = soma_entrada + comissao_total
            percentual_entrada = entrada_total / soma_credito

            if abs(soma_credito - credito_desejado) / credito_desejado <= 0.05:
                if percentual_entrada <= entrada_max + 0.05:
                    opcoes.append({
                        "Crédito Total": round(soma_credito, 2),
                        "Entrada Total": round(entrada_total, 2),
                        "Percentual Entrada": f"{round(percentual_entrada * 100, 2)}%",
                        "Cartas Utilizadas": combinacao
                    })
        if len(opcoes) >= 5:
            break

    return opcoes[:5]
