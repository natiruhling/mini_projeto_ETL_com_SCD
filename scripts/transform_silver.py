import pandas as pd
import unicodedata

def limpar_colunas(col):
    """
    Padroniza nomes de colunas para uso no BigQuery.

    Regras aplicadas:
    - Remove acentos
    - Converte para minúsculo
    - Substitui espaços por "_"
    - Remove pontos e caracteres especiais básicos
    """
    # Remove acentos
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('utf-8')

    # Converte para minúsculo
    col = col.lower()

    # Substitui espaços por underscore
    col = col.replace(" ", "_")

    # Remove pontos
    col = col.replace(".", "")

    return col


def transform_produtos(path):
    """
    Realiza a transformação dos dados de produtos (camada silver).

    Etapas:
    - Leitura do Excel
    - Padronização de colunas (remoção de acentos, espaços e caracteres inválidos)
    - Remoção de duplicados
    - Criação da coluna valor_produto (proxy de valor de mercado)
    - Conversão de tipos
    - Padronização do ID do produto
    """

    df = pd.read_excel(path)

    # Padroniza nomes das colunas
    df.columns = [limpar_colunas(c) for c in df.columns]

    # Remove duplicados
    df = df.drop_duplicates()

    # =========================
    # CRIAR valor_produto (ESSENCIAL PRO SCD)
    # =========================

    col1 = 'tipo_informacao_si_bandeira_concorrente_unidade'
    col2 = 'tipo_informacao_so_bandeira_preco_popular_unidade'

    if col1 in df.columns and col2 in df.columns:
        df['valor_produto'] = (
            df[col1].fillna(0) +
            df[col2].fillna(0)
        ) / 2
    else:
        print("⚠️ Colunas de base para valor_produto não encontradas")

    # Garante tipo numérico
    df['valor_produto'] = pd.to_numeric(df['valor_produto'], errors='coerce').round(2)

    # =========================
    # VALIDAR ID
    # =========================

    if 'cod_prod_catarinense' in df.columns:
        print("cod_prod_catarinense é único?:", df['cod_prod_catarinense'].is_unique)
    else:
        print("⚠️ coluna cod_prod_catarinense não encontrada")

    # =========================
    # CRIAR id_produto_original
    # =========================

    if 'id_produto_original' not in df.columns:
        df['id_produto_original'] = df['cod_prod_catarinense'].astype(str)

    return df

def transform_filiais(path):
    """
    Transformação dos dados de filiais (camada silver).

    Etapas:
    - Leitura do Excel
    - Padronização de colunas (remoção de acentos, espaços e caracteres inválidos)
    - Remoção de duplicados
    """

    df = pd.read_excel(path)

    # Padroniza nomes das colunas
    df.columns = [limpar_colunas(c) for c in df.columns]

    # Remove duplicados
    df = df.drop_duplicates()

    return df