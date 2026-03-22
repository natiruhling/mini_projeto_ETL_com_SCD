from google.cloud import storage

def upload_to_gcs(bucket_name, source_file, destination_blob):
    """
    Faz upload de um arquivo local para um bucket no Google Cloud Storage.

    Parâmetros:
    - bucket_name (str): Nome do bucket no GCS.
    - source_file (str): Caminho do arquivo local que será enviado.
    - destination_blob (str): Caminho/nome do arquivo no bucket (destino no GCS).

    Exemplo:
    upload_to_gcs(
        bucket_name="meu-bucket",
        source_file="dados/vendas.csv",
        destination_blob="raw/vendas/vendas.csv"
    )
    """

    # Cria o cliente de conexão com o GCS usando a conta de serviço
    client = storage.Client.from_service_account_json("credenciais.json")
    
    # Acessa o bucket especificado
    bucket = client.bucket(bucket_name)
    
    # Cria uma referência (blob) para o arquivo dentro do bucket
    blob = bucket.blob(destination_blob)

    # Faz o upload do arquivo local para o GCS
    blob.upload_from_filename(source_file)

    # Mensagem de confirmação
    print(f"Arquivo {source_file} enviado para {destination_blob}.")