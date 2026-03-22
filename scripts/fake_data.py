import pandas as pd
import random

def gerar_novos_dados(df):
    """
    Simula alterações de valor em produtos para teste de SCD Tipo 2.

    Regras:
    - Cria a coluna 'valor_produto' como proxy de valor de mercado
      usando média de vendas entre concorrentes
    - Aplica variação aleatória em parte dos registros (30%)
    """

    df_fake = df.copy()

    # =========================
    # 1. CRIAR valor_produto (PROXY DE VALOR)
    # =========================

    if 'valor_produto' not in df_fake.columns:

        col1 = 'tipo_informacao_si_bandeira_concorrente_unidade'
        col2 = 'tipo_informacao_so_bandeira_preco_popular_unidade'

        if col1 in df_fake.columns and col2 in df_fake.columns:
            df_fake['valor_produto'] = (
                df_fake[col1].fillna(0) +
                df_fake[col2].fillna(0)
            ) / 2

        else:
            # fallback caso alguma coluna não exista
            df_fake['valor_produto'] = [random.uniform(10, 100) for _ in range(len(df_fake))]

    # =========================
    # 2. SIMULAR ALTERAÇÃO (SCD)
    # =========================

    # Seleciona 30% dos produtos
    amostra = df_fake.sample(frac=0.3, random_state=42).index

    for i in amostra:
        if pd.notnull(df_fake.loc[i, 'valor_produto']):
            df_fake.loc[i, 'valor_produto'] *= random.uniform(1.1, 1.5)
            
    # Garantir que é numérico
    df_fake['valor_produto'] = pd.to_numeric(df_fake['valor_produto'], errors='coerce').round(2)

    return df_fake