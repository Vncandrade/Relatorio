import pandas as pd
import psycopg2
import matplotlib.pyplot as plt # type: ignore
from dotenv import load_dotenv
import os
from datetime import datetime

#Config do pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.width', None)

load_dotenv()

try:
    connect = psycopg2.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    print("Conexão estabelecida com sucesso!")

    query_vendas = """
        SELECT
            bf.codigoproduto,
            bp.nomeproduto,
            bp.marca,
            -- Setembro 2025
            SUM(bf.precounitario * bf.quantidadenegociada) AS faturamento,
            
            -- Setembro 2024
            SUM(CASE WHEN EXTRACT(YEAR FROM bfa.datafaturamento) = 2024 
                    AND EXTRACT(MONTH FROM bfa.datafaturamento) = 9 
                    THEN bfa.precounitario * bfa.quantidadenegociada ELSE 0 END) AS faturamentosply,

            MAX(bf.datafaturamento) AS ultima_venda,

            -- Subquery último estoque
            (SELECT be.estoquenadata 
            FROM dbo.bi_estoque be 
            WHERE be.codigoprincipal = bf.codigoproduto 
            ORDER BY be.data DESC 
            LIMIT 1) AS estoque_na_data,

            CASE WHEN pb.permitecompra = TRUE THEN '[✓]' ELSE '[ ]' END AS P_Compra,
            CASE WHEN pb.permitevenda = TRUE THEN '[✓]' ELSE '[ ]' END AS P_Venda, 
            CASE WHEN pb.inativo = TRUE THEN '[✓]' ELSE '[ ]' END AS Inativo

        FROM dbo.bi_fato AS bf
        INNER JOIN dbo.bi_produto AS bp ON bf.codigoproduto = bp.codigoproduto
        INNER JOIN dbo.produtobase AS pb ON bf.codigoproduto = pb.codigoprincipal
        LEFT JOIN dbo.bi_fato_antigo AS bfa 
            ON bf.codigoproduto = bfa.codigoproduto
            AND EXTRACT(YEAR FROM bfa.datafaturamento) = 2024
            AND EXTRACT(MONTH FROM bfa.datafaturamento) = 9
            AND bfa.tipomovumento IN ('V', 'B')
        WHERE
            bf.tipomovumento IN ('V', 'B')
            AND EXTRACT(YEAR FROM bf.datafaturamento) = 2025 
            AND EXTRACT(MONTH FROM bf.datafaturamento) = 9
        GROUP BY
            bf.codigoproduto, bp.nomeproduto, bp.marca, 
            pb.permitecompra, pb.permitevenda, pb.inativo
        HAVING 
            SUM(bf.precounitario * bf.quantidadenegociada) > 0 
        ORDER BY faturamento DESC;
"""

    # Executar query
    df_produto = pd.read_sql(query_vendas, connect)

    # Ordenar por faturamento 2025
    df_produto = df_produto.sort_values('faturamento', ascending=False)

    # Calcular valor acumulado (baseado em 2025)
    df_produto['PorcentAcumulado'] = (
        df_produto['faturamento'].cumsum() /  
        df_produto['faturamento'].sum() * 100  
    )

    # Classificação ABC
    def classificar_abc(p):
        if p <= 80:
            return 'A'
        elif p <= 95:
            return 'B'
        else:
            return 'C'

    # Coluna ABC
    df_produto['Classificacao'] = df_produto['PorcentAcumulado'].apply(classificar_abc)

    # Calcular crescimento vs ano anterior
    df_produto['Crescimento'] = df_produto.apply(
        lambda x: ((x['faturamento'] - x['faturamentosply']) / x['faturamentosply'] * 100).round(2) 
        if x['faturamentosply'] > 0 else None, 
        axis=1
    )

    # Distribuição ABC
    print(f"\n🎯 Distribuição ABC:")
    print(df_produto['Classificacao'].value_counts())

    # Exportar para Excel
    data_atual = datetime.now().strftime("%Y-%m-%d_%H-%M")
    nome_arquivo = f"CurvaABC_Setembro2025_vs_2024_{data_atual}.xlsx"

    df_produto.to_excel(nome_arquivo, index=False)
    print(f"✅ Arquivo exportado: {nome_arquivo}")

except psycopg2.Error as e:
    print("Erro ao conectar:", e)

except Exception as e:
    print(f"Erro durante execução: {e}")

finally:
    if connect:
        connect.close()
        print("Conexão fechada")