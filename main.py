from scripts.ingestion_bronze import upload_to_gcs
from scripts.transform_silver import transform_produtos, transform_filiais
from scripts.load_gold import load_to_bigquery
from scripts.scd_type2 import aplicar_scd_tipo2
from scripts.fake_data import gerar_novos_dados
import pandas as pd

bucket_name = "bucket-etl-clamed-nathalia"

# Upload arquivos
upload_to_gcs(
    bucket_name,
    "data/raw/MS_12_2022_sample.xlsx",
    "bronze/MS_12_2022.xlsx"
)

upload_to_gcs(
    bucket_name,
    "data/raw/filial-brick_sample.xlsx",
    "bronze/filial.xlsx"
)

# Transformar
df_produtos = transform_produtos("data/raw/MS_12_2022_sample.xlsx")
df_filiais = transform_filiais("data/raw/filial-brick_sample.xlsx")

# Salvar Silver local
df_produtos.to_csv("data/processed/produtos_silver.csv", index=False)
df_filiais.to_csv("data/processed/filiais_silver.csv", index=False)

print("Transformação concluída")

project_id = "mini-etl-scd"
dataset = "dataset_clamed"

# Tabelas
table_produtos = f"{project_id}.{dataset}.produtos_silver"
table_filiais = f"{project_id}.{dataset}.filiais_silver"

# Subir dados para DW BigQuery
load_to_bigquery(df_produtos, table_produtos)
load_to_bigquery(df_filiais, table_filiais)

# =========================
# SCD TIPO 2 - GOLD
# =========================

print("Iniciando SCD Tipo 2...")

# Primeira carga 
df_gold = aplicar_scd_tipo2(df_produtos)

# Simular nova carga com alteração
df_novos = gerar_novos_dados(df_produtos)

# Aplicar SCD novamente
df_gold = aplicar_scd_tipo2(df_novos, df_gold)

# Salvar local
df_gold.to_csv("data/processed/produtos_gold.csv", index=False)

print("SCD aplicado com sucesso")

table_gold = f"{project_id}.{dataset}.produtos_gold"

load_to_bigquery(df_gold, table_gold)
print(df_gold.head())