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

    query_2024 = """
    SELECT
        -- 👇 LÓGICA: Se grupo vazio, usa nome do cliente
        CASE 
            WHEN bc.grupoeconomico IS NULL OR bc.grupoeconomico = '' 
            THEN bc.nomecliente 
            ELSE bc.grupoeconomico 
        END AS grupo_ou_cliente,
        
        COUNT(DISTINCT bc.codigocliente) AS qtd_clientes,
        SUM(bfa.precounitario * bfa.quantidadenegociada) AS faturamento,
        COUNT(bfa.datafaturamento) AS Qtd_Pedidos,
        SUM(bfa.quantidadenegociada) AS Qtd_Itens_Total,
        CEILING(SUM(bfa.quantidadenegociada) / NULLIF(COUNT(bfa.datafaturamento), 0)) AS Itens_por_Pedidos,
        MAX(bfa.datafaturamento) AS ultima_venda,
        
        -- 👇 Identificar se é grupo real ou cliente individual
        CASE 
            WHEN bc.grupoeconomico IS NULL OR bc.grupoeconomico = '' 
            THEN 'Cliente Individual' 
            ELSE 'Grupo Econômico' 
        END AS tipo_agrupamento

    FROM
        dbo.bi_fato_antigo AS bfa  -- 👈 USANDO TABELA ANTIGA
    INNER JOIN
        dbo.bi_cliente AS bc ON bfa.codigocliente = bc.codigocliente
    WHERE
        bfa.tipomovumento IN ('V', 'B')
        -- 👇 PERÍODO ESPECÍFICO: 01/01/2024 até 09/10/2024
        AND bfa.datafaturamento >= '2024-01-01'
        AND bfa.datafaturamento <= '2024-10-09'
        AND bc.uf = 'RJ'
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
    df_2024 = pd.read_sql(query_2024, connect)

    # Ordenar por faturamento
    df_2024 = df_2024.sort_values('faturamento', ascending=False)

    # Calcular valor acumulado
    df_2024['PorcentAcumulado'] = (
        df_2024['faturamento'].cumsum() /  
        df_2024['faturamento'].sum() * 100  
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
    df_2024['Classificacao'] = df_2024['PorcentAcumulado'].apply(classificar_abc)

    # Verificar os dados
    print("📊 Dados de 2024 carregados!")
    print(f"Período: 01/01/2024 até 09/10/2024")
    print(f"Total de grupos/clientes: {len(df_2024)}")
    print(f"Grupos econômicos: {len(df_2024[df_2024['tipo_agrupamento'] == 'Grupo Econômico'])}")
    print(f"Clientes individuais: {len(df_2024[df_2024['tipo_agrupamento'] == 'Cliente Individual'])}")
    print(f"Faturamento total RJ (2024): R$ {df_2024['faturamento'].sum():,.2f}")
    
    print("\n📋 Top Grupos/Clientes (2024):")
    print(df_2024[['grupo_ou_cliente', 'tipo_agrupamento', 'faturamento', 'Classificacao']].head(10))

    # Mostrar distribuição ABC
    print(f"\n🎯 Distribuição ABC (2024):")
    distribuicao = df_2024['Classificacao'].value_counts()
    print(distribuicao)

    # Exportar para Excel
    data_atual = datetime.now().strftime("%Y-%m-%d_%H-%M")
    nome_arquivo = f"Analise_2024_RJ_{data_atual}.xlsx"

    df_2024.to_excel(nome_arquivo, index=False)
    print(f"✅ Arquivo exportado: {nome_arquivo}")

except psycopg2.Error as e:
    print("Erro ao conectar:", e)

except Exception as e:
    print(f"Erro durante execução: {e}")

finally:
    if connect:
        connect.close()
        print("Conexão fechada")