📊 Mini Projeto ETL com SCD Tipo 2 (GCP + Python)
📌 Objetivo

Desenvolver um pipeline de dados utilizando Python e Google Cloud Platform para simular o processamento de dados do setor farmacêutico, aplicando a técnica de SCD Tipo 2 (Slowly Changing Dimension) para controle de histórico.

🏗️ Arquitetura do Projeto

O pipeline foi estruturado em três camadas:

🟤 Bronze (Ingestão)
Upload de arquivos .xlsx para o Google Cloud Storage
Simula ingestão de dados brutos

⚪ Silver (Transformação)
Limpeza e padronização dos dados
Remoção de duplicidades
Criação de coluna derivada valor_produto
Padronização de tipos e nomes de colunas

🟡 Gold (Data Warehouse)
Implementação de SCD Tipo 2
Controle de histórico de alterações
Armazenamento final no BigQuery

🔁 Regras do SCD Tipo 2
✔ Novo produto → inserido
✔ Produto alterado → nova linha criada
✔ Registro anterior → marcado como inativo
✔ Histórico preservado

🧾 Estrutura da Tabela Final
Coluna	Descrição
sk_produto	Chave substituta
id_produto_original	ID natural do produto
valor_produto	Valor calculado do produto
data_inicio_validade	Início da validade
data_fim_validade	Fim da validade
flag_ativo	Indica registro atual

🛠️ Tecnologias Utilizadas
Python
Pandas
Google Cloud Storage (GCS)
BigQuery
VS Code

📂 Estrutura do Projeto
mini_projeto_ETL_com_SCD/
│
├── data/
│   ├── raw/                # Arquivos originais (.xlsx)
│   └── processed/          # Dados tratados (Silver/Gold)
│
├── scripts/
│   ├── ingestion_bronze.py
│   ├── transform_silver.py
│   ├── fake_data.py
│   ├── scd_type2.py
│   └── load_gold.py
│
├── credenciais.json
├── main.py
├── requirements.txt
└── README.md

⚙️ Como Executar o Projeto

1. Criar ambiente virtual
python -m venv venv
venv\Scripts\activate

2. Instalar dependências
pip install -r requirements.txt

3. Configurar credenciais GCP
Criar projeto no GCP
Criar Service Account
Baixar credenciais.json
Colocar na raiz do projeto

4. Executar pipeline
python main.py

🔍 Validação no BigQuery

🔹 Ver histórico de alterações
SELECT 
SELECT 
  id_produto_original,
  valor_produto,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_inicio_validade) as inicio,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_fim_validade) as fim,
  flag_ativo
FROM `mini-etl-scd.dataset_clamed.produtos_gold`
ORDER BY id_produto_original, data_inicio_validade;

🔹 Ver antes vs depois
SELECT 
  antigo.id_produto_original,
  antigo.valor_produto AS valor_antigo,
  novo.valor_produto AS valor_novo
FROM `mini-etl-scd.dataset_clamed.produtos_gold` antigo
JOIN `mini-etl-scd.dataset_clamed.produtos_gold` novo
  ON antigo.id_produto_original = novo.id_produto_original
WHERE 
  antigo.flag_ativo = FALSE
  AND novo.flag_ativo = TRUE

💡 Principais Aprendizados
Implementação de pipeline ETL em nuvem
Uso de arquitetura Bronze → Silver → Gold
Aplicação prática de SCD Tipo 2
Boas práticas de tratamento de dados
Integração com BigQuery

🚀 Possíveis Melhorias
Orquestração com Airflow
Uso de dbt para transformações
Criação de dashboards (Looker Studio)
Particionamento de tabelas no BigQuery

👩‍💻 Autora

Projeto desenvolvido por Nathália Rühling Rocha como parte de estudo em Engenharia de Dados.