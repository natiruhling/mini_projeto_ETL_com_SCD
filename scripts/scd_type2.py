import pandas as pd
from datetime import datetime

def aplicar_scd_tipo2(df_novo, df_atual=None):
    """
    Aplica SCD Tipo 2 para controle de histórico de produtos.

    Regras:
    - Novo produto → inserido
    - Produto alterado → fecha registro antigo e cria novo
    - Produto igual → ignora
    """

    hoje = datetime.now().replace(microsecond=0)

    # Primeira carga
    if df_atual is None or df_atual.empty:
        df_novo['data_inicio_validade'] = hoje
        df_novo['data_fim_validade'] = None
        df_novo['flag_ativo'] = True
        df_novo['sk_produto'] = range(1, len(df_novo) + 1)

        return df_novo

    df_final = df_atual.copy()

    for _, row in df_novo.iterrows():
        id_prod = row['id_produto_original']
        valor_novo = row['valor_produto']

        registro_ativo = df_final[
            (df_final['id_produto_original'] == id_prod) &
            (df_final['flag_ativo'] == True)
        ]

        if registro_ativo.empty:
            # Novo produto
            novo = row.copy()
            novo['data_inicio_validade'] = hoje
            novo['data_fim_validade'] = None
            novo['flag_ativo'] = True
            novo['sk_produto'] = df_final['sk_produto'].max() + 1

            df_final = pd.concat([df_final, pd.DataFrame([novo])])

        else:
            valor_antigo = registro_ativo.iloc[0]['valor_produto']

            if valor_novo != valor_antigo:
                # Fecha registro antigo
                idx = registro_ativo.index[0]
                df_final.loc[idx, 'data_fim_validade'] = hoje
                df_final.loc[idx, 'flag_ativo'] = False

                # Cria novo registro
                novo = row.copy()
                novo['data_inicio_validade'] = hoje
                novo['data_fim_validade'] = None
                novo['flag_ativo'] = True
                novo['sk_produto'] = df_final['sk_produto'].max() + 1

                df_final = pd.concat([df_final, pd.DataFrame([novo])])

    return df_final