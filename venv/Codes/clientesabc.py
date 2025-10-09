import pandas as pd
import psycopg2
import matplotlib.pyplot as plt # type: ignore
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

    query_vendas = """
    SELECT
        -- 👇 LÓGICA: Se grupo vazio, usa nome do cliente
        CASE 
            WHEN bc.grupoeconomico IS NULL OR bc.grupoeconomico = '' 
            THEN bc.nomecliente 
            ELSE bc.grupoeconomico 
        END AS grupo_ou_cliente,
        
        COUNT(DISTINCT bc.codigocliente) AS qtd_clientes,
        SUM(bf.precounitario * bf.quantidadenegociada) AS faturamento,
        COUNT(bf.datafaturamento) AS Qtd_Pedidos,
        SUM(bf.quantidadenegociada) AS Qtd_Itens_Total,
        CEILING(SUM(bf.quantidadenegociada) / NULLIF(COUNT(bf.datafaturamento), 0)) AS Itens_por_Pedidos,
        MAX(bf.datafaturamento) AS ultima_venda,
        
        -- 👇 Identificar se é grupo real ou cliente individual
        CASE 
            WHEN bc.grupoeconomico IS NULL OR bc.grupoeconomico = '' 
            THEN 'Cliente Individual' 
            ELSE 'Grupo Econômico' 
        END AS tipo_agrupamento

    FROM
        dbo.bi_fato AS bf
    INNER JOIN
        dbo.bi_cliente AS bc ON bf.codigocliente = bc.codigocliente
    WHERE
        bf.tipomovumento IN ('V', 'B')
        AND bf.datafaturamento >= CURRENT_DATE - INTERVAL '3 months'
        AND bc.uf = 'RJ'
        AND bc.grupoeconomico <> 'BMB MATERIAL'
    GROUP BY
        -- 👇 Agrupar pela mesma lógica do CASE
        CASE 
            WHEN bc.grupoeconomico IS NULL OR bc.grupoeconomico = '' 
            THEN bc.nomecliente 
            ELSE bc.grupoeconomico 
        END,
        CASE 
            WHEN bc.grupoeconomico IS NULL OR bc.grupoeconomico = '' 
            THEN 'Cliente Individual' 
            ELSE 'Grupo Econômico' 
        END
    ORDER BY
        faturamento DESC;
"""

    # Executar a query
    df_consolidado = pd.read_sql(query_vendas, connect)

    # Ordenar por faturamento
    df_consolidado = df_consolidado.sort_values('faturamento', ascending=False)

    # Calcular valor acumulado
    df_consolidado['PorcentAcumulado'] = (
        df_consolidado['faturamento'].cumsum() /  
        df_consolidado['faturamento'].sum() * 100  
    )

    # Função de classificação ABC
    def classificar_abc(p):
        if p <= 80:
            return 'A'
        elif p <= 95:
            return 'B'
        else:
            return 'C'

    # Adicionar coluna de classificação ABC
    df_consolidado['Classificacao'] = df_consolidado['PorcentAcumulado'].apply(classificar_abc)

    # Verificar os dados
    print("📊 Dados consolidados carregados!")
    print(f"Total de grupos/clientes: {len(df_consolidado)}")
    print(f"Grupos econômicos: {len(df_consolidado[df_consolidado['tipo_agrupamento'] == 'Grupo Econômico'])}")
    print(f"Clientes individuais: {len(df_consolidado[df_consolidado['tipo_agrupamento'] == 'Cliente Individual'])}")
    print(f"Faturamento total RJ (3 meses): R$ {df_consolidado['faturamento'].sum():,.2f}")
    
    print("\n📋 Top Grupos/Clientes:")
    print(df_consolidado[['grupo_ou_cliente', 'tipo_agrupamento', 'faturamento', 'Classificacao']].head(10))

    # Mostrar distribuição ABC
    print(f"\n🎯 Distribuição ABC:")
    distribuicao = df_consolidado['Classificacao'].value_counts()
    print(distribuicao)

    # Exportar para Excel
    data_atual = datetime.now().strftime("%Y-%m-%d_%H-%M")
    nome_arquivo = f"Analise_Consolidada_RJ_{data_atual}.xlsx"

    df_consolidado.to_excel(nome_arquivo, index=False)
    print(f"✅ Arquivo exportado: {nome_arquivo}")

except psycopg2.Error as e:
    print("Erro ao conectar:", e)

except Exception as e:
    print(f"Erro durante execução: {e}")

finally:
    if connect:
        connect.close()
        print("Conexão fechada")