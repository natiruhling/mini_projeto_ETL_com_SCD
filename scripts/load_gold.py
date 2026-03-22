from google.cloud import bigquery

def load_to_bigquery(df, table_id):
    """
    Carrega um DataFrame do pandas para uma tabela no Google BigQuery.

    Etapas realizadas:
    - Autenticação com o BigQuery usando conta de serviço
    - Envio (load) do DataFrame para a tabela de destino
    - Aguarda a finalização do job de carregamento

    Parâmetros:
    - df (pd.DataFrame): DataFrame contendo os dados a serem carregados
    - table_id (str): Identificador completo da tabela no BigQuery
                      (formato: "projeto.dataset.tabela")
    """

    # Cria o cliente do BigQuery autenticando via arquivo de credenciais
    client = bigquery.Client.from_service_account_json("credenciais.json")

    # Cria um job de carregamento do DataFrame para a tabela
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"  # Sobrescreve a tabela se já existir
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    # Aguarda a execução completa do job (importante para garantir consistência)
    job.result()

    # Mensagem de confirmação
    print(f"Tabela carregada no BigQuery: {table_id}")