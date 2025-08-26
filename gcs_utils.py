from google.cloud import storage

def baixar_arquivo_gcs(source_blob_name, destination_file_name, credentials_path):
    # Cria o cliente de storage com as credenciais
    storage_client = storage.Client.from_service_account_json(credentials_path)

    # Nome do bucket
    bucket_name = "planilhas-codecalc"

    # Acessa o bucket
    bucket = storage_client.bucket(bucket_name)

    # Acessa o arquivo no bucket
    blob = bucket.blob(source_blob_name)

    # Faz o download para o nome de destino
    blob.download_to_filename(destination_file_name)

    print(f"Arquivo {source_blob_name} baixado para {destination_file_name}.")
