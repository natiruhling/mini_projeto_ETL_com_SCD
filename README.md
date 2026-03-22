# 📊 Mini Projeto ETL com SCD Tipo 2 (GCP + Python)

## 📌 Objetivo

Implementar um pipeline de ETL com Python e Google Cloud Platform para simular processamento de dados do setor farmacêutico e aplicar Slowly Changing Dimension (SCD) Tipo 2.

## 🏗️ Visão Geral da Arquitetura

Camadas:
- 🟤 **Bronze**: ingestão de dados brutos (.xlsx) no Google Cloud Storage.
- ⚪ **Silver**: transformação, limpeza, padronização e geração de dados consistentes.
- 🟡 **Gold**: modelo dimensional com SCD Tipo 2 e histórico preservado (BigQuery).

## 🔁 SCD Tipo 2 (Regras aplicadas)

- ✅ Novo registro: adiciona nova linha.
- ✅ Registro existente com mudança: nova linha ativa + anterior inativo.
- ✅ Histórico preservado com validade (data_início/data_fim).
- ✅ `flag_ativo` indica linha atual.

## 🧾 Modelo de Tabela (Gold)

| Coluna               | Tipo     | Descrição                          |
|---------------------|----------|------------------------------------|
| `sk_produto`        | INT      | Chave substituta (surrogate key)  |
| `id_produto_original`| INT      | Chave natural do produto          |
| `valor_produto`     | FLOAT    | Valor calculado (ajustes)         |
| `data_inicio_validade` | TIMESTAMP | Validade início                   |
| `data_fim_validade` | TIMESTAMP | Validade fim (NULL se ativo)      |
| `flag_ativo`        | BOOL     | Linha atual (TRUE/FALSE)          |

## 🛠️ Tecnologias

- Python 3.x
- pandas
- Google Cloud Storage
- BigQuery
- VS Code

## 📂 Estrutura do Projeto

```
mini_projeto_ETL_com_SCD/
├── data/
│   ├── raw/                # Dados de entrada (.xlsx)
│   └── processed/          # Resultados Silver/Gold (.csv)
├── scripts/
│   ├── fake_data.py
│   ├── ingestion_bronze.py
│   ├── transform_silver.py
│   ├── scd_type2.py
│   └── load_gold.py
├── credenciais.json
├── main.py
├── requirements.txt
└── README.md
```

## ⚙️ Como executar

1. Criar e ativar ambiente virtual

```powershell
python -m venv venv
venv\Scripts\activate
```

2. Instalar dependências

```powershell
pip install -r requirements.txt
```

3. Configurar GCP
- Criar projeto no GCP
- Criar Service Account
- Baixar `credenciais.json`
- Salvar na raiz do projeto

4. Executar pipeline

```powershell
python main.py
```

## 🔍 Exemplos de validação no BigQuery

### Ver histórico por produto

```sql
SELECT
  id_produto_original,
  valor_produto,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_inicio_validade) AS inicio,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_fim_validade) AS fim,
  flag_ativo
FROM `mini-etl-scd.dataset_clamed.produtos_gold`
ORDER BY id_produto_original, data_inicio_validade;
```

### Identificar alterações (antes vs depois)

```sql
SELECT
  antigo.id_produto_original,
  antigo.valor_produto AS valor_antigo,
  novo.valor_produto AS valor_novo
FROM `mini-etl-scd.dataset_clamed.produtos_gold` antigo
JOIN `mini-etl-scd.dataset_clamed.produtos_gold` novo
  ON antigo.id_produto_original = novo.id_produto_original
WHERE
  antigo.flag_ativo = FALSE
  AND novo.flag_ativo = TRUE;
```

## 💡 Aprendizados

- Criação de pipeline ETL em nuvem (GCP)
- Uso de arquitetura Bronze → Silver → Gold
- Implementação prática de SCD Tipo 2
- Tratamento e controle de qualidade de dados
- Integração com BigQuery (armazém de dados)

## 🚀 Possíveis melhorias

- Orquestração (Apache Airflow / Cloud Composer)
- Transformações versionadas com dbt
- Dashboards no Looker Studio
- Particionamento e clustering no BigQuery
- Testes automatizados e validação de dados

## 👩‍💻 Autora

Nathália Rühling Rocha

Projeto criado para estudos em Engenharia de Dados.
