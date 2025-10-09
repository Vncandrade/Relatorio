import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os
from datetime import datetime

# Configurações do pandas
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

    query = """
    SELECT
        bv.nomerepresentante,
        -- Faturamento por marca específica
        SUM(CASE WHEN UPPER(bp.marca) = 'MECTRONIC' THEN bf.precounitario * bf.quantidadenegociada ELSE 0 END) AS faturamento_mectronic,
        SUM(CASE WHEN UPPER(bp.marca) = 'FORTLEV' THEN bf.precounitario * bf.quantidadenegociada ELSE 0 END) AS faturamento_fortlev,
        SUM(CASE WHEN UPPER(bp.marca) LIKE '%HYDRONORTH%' THEN bf.precounitario * bf.quantidadenegociada ELSE 0 END) AS faturamento_hydronorth,
        -- Faturamento total
        SUM(bf.precounitario * bf.quantidadenegociada) AS faturamento_total
    FROM
        dbo.bi_fato AS bf
    INNER JOIN dbo.bi_produto AS bp
        ON bf.codigoproduto = bp.codigoproduto
    INNER JOIN dbo.bi_vendedor AS bv 
        ON bf.codigovendedor = bv.codigovendedor
    WHERE
        bf.tipomovumento IN ('V', 'B')
        AND EXTRACT(MONTH FROM bf.datafaturamento) = EXTRACT(MONTH FROM CURRENT_DATE)
        AND EXTRACT(YEAR  FROM bf.datafaturamento) = EXTRACT(YEAR  FROM CURRENT_DATE)
        AND   (UPPER(bp.marca) LIKE '%MECTRONIC%' 
            OR UPPER(bp.marca) LIKE '%FORTLEV%'
            OR UPPER(bp.marca) LIKE '%HYDRONORTH%')
    GROUP BY 
        bv.nomerepresentante
    ORDER BY
        faturamento_total DESC;
    """

    # Executar a query e criar DataFrame
    df_vendas = pd.read_sql(query, connect) 
    
    # Verificar os dados
    print("📊 Dados de vendas carregados!")
    print(f"Total de vendedores: {len(df_vendas)}")
    print("\n📋 Resultados consolidados por representante:")
    print(df_vendas.head(10))

    # Estatísticas resumidas
    print(f"\n🎯 Estatísticas Gerais:")
    print(f"Total MECTRONIC: R$ {df_vendas['faturamento_mectronic'].sum():,.2f}")
    print(f"Total FORTLEV: R$ {df_vendas['faturamento_fortlev'].sum():,.2f}")
    print(f"Total HYDRONORTH: R$ {df_vendas['faturamento_hydronorth'].sum():,.2f}")
    print(f"💰 Faturamento Total Geral: R$ {df_vendas['faturamento_total'].sum():,.2f}")

    # Exportar para Excel
    data_atual = datetime.now().strftime("%Y-%m-%d_%H-%M")
    nome_arquivo = f"Vendas_Representantes_7dias_{data_atual}.xlsx"

    df_vendas.to_excel(nome_arquivo, index=False)
    print(f"\n✅ Arquivo exportado: {nome_arquivo}")

except psycopg2.Error as e:
    print("Erro ao conectar:", e)

except Exception as e:
    print(f"Erro durante execução: {e}")

finally:
    if 'connect' in locals() and connect:
        connect.close()
        print("Conexão fechada")
    print("Fim da execução")