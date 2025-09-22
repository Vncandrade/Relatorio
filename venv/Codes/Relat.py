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
        bf.datafaturamento,
        bc.codigocliente,
        bc.nomecliente,
        bv.nomerepresentante,
        SUM(bf.precounitario * bf.quantidadenegociada) AS faturamento
    FROM
        dbo.bi_fato AS bf
    INNER JOIN 
        dbo.bi_cliente AS bc 
        ON bf.codigocliente = bc.codigocliente
    INNER JOIN 
        dbo.bi_vendedor AS bv 
        ON bc.codigovendedor = bv.codigovendedor
    WHERE
        bf.tipomovumento IN ('V', 'B')
        AND bf.datafaturamento > '2025-09-18'
    GROUP BY
        bf.datafaturamento,
        bc.codigocliente, 
        bc.nomecliente, 
        bv.nomerepresentante
    ORDER BY
        faturamento DESC;
    """

    # Executar a query e criar DataFrame
    df_vendas = pd.read_sql_query(query, connect)
    
    # 3. Verificar os dados
    print("📊 Dados de vendas carregados!")
    print(f"Total de clientes: {len(df_vendas)}")
    print("\nPrimeiras linhas:")
    print(df_vendas.head(10))

    # Classificação ABC (exemplo básico)
    df_vendas = df_vendas.sort_values('faturamento', ascending=False)
    df_vendas['cumulative_sum'] = df_vendas['faturamento'].cumsum()
    df_vendas['cumulative_perc'] = 100 * df_vendas['cumulative_sum'] / df_vendas['faturamento'].sum()
    
    # Criar classificação ABC
    df_vendas['Classificacao'] = 'C'
    df_vendas.loc[df_vendas['cumulative_perc'] <= 80, 'Classificacao'] = 'A'
    df_vendas.loc[(df_vendas['cumulative_perc'] > 80) & (df_vendas['cumulative_perc'] <= 95), 'Classificacao'] = 'B'

    # ✅ MOSTRAR DISTRIBUIÇÃO ABC
    print(f"\n🎯 Distribuição ABC:")
    print(df_vendas['Classificacao'].value_counts())

    # Exportar para Excel
    data_atual = datetime.now().strftime("%Y-%m-%d_%H-%M")
    nome_arquivo = f"Ultimas_Vendas_{data_atual}.xlsx"

    df_vendas.to_excel(nome_arquivo, index=False)
    print(f"✅ Arquivo exportado: {nome_arquivo}")
    print(f"📊 Total de clientes analisados: {len(df_vendas)}")

except psycopg2.Error as e:
    print("Erro ao conectar:", e)

except Exception as e:
    print(f"Erro durante execução: {e}")

finally:
    if 'connect' in locals() and connect:
        connect.close()
        print("Conexão fechada")
    print("Fim da execução")