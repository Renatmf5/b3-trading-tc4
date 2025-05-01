import pandas as pd
import os
import yfinance as yf
vol =  pd.read_parquet("dados/indicadores/vol_252.parquet")
vol
acoes_cotacoes = pd.read_parquet("dados/acoes/acoes_cotacoes.parquet")
acoes_cotacoes

batch_files = [
            os.path.join("dados/balancos", file)
            for file in os.listdir("dados/balancos")
            if file.startswith("lote_") and file.endswith(".parquet")
        ]

if not batch_files:
    print("Nenhum lote encontrado para concatenação.")

# Concatenar todos os lotes
df_consolidado = pd.concat([pd.read_parquet(batch_file) for batch_file in batch_files], ignore_index=True)
df_consolidado.loc[df_consolidado['valor_primeiro_periodo'] == 0, 'valor_primeiro_periodo'] = None



# Garantir que a coluna data_doc esteja no formato datetime
if 'data_doc' in df_consolidado.columns:
    df_consolidado['data_doc'] = pd.to_datetime(df_consolidado['data_doc'], format='%d/%m/%Y', errors='coerce')

# Ordenar o DataFrame por ticker, conta e data_doc de forma decrescente
df_consolidado = df_consolidado.sort_values(by=["ticker", "conta", "data_doc"], ascending=[True, True, True])


# Preencher valores ausentes no valor_primeiro_periodo com o último valor disponível dentro de cada grupo (ticker e conta)
if 'valor_primeiro_periodo' in df_consolidado.columns:
    df_consolidado['valor_primeiro_periodo'] = (
        df_consolidado.groupby(['ticker', 'conta'], group_keys=False)['valor_primeiro_periodo']
        .apply(lambda group: group.fillna(method='ffill'))  # Preenche apenas dentro do grupo
    )

# Filtrar apneas os periodos do ultimos trimestre de cada ano
df_ultimo_trimeste = df_consolidado[df_consolidado['data_doc'].dt.month == 12]

# Ajustar quarto trimestre
# Adicionar uma coluna para o ano
df_consolidado['ano'] = df_consolidado['data_doc'].dt.year

# Adicionar uma coluna para o trimestre
df_consolidado['trimestre'] = ((df_consolidado['data_doc'].dt.month - 1) // 3) + 1

# Lista de contas que devem ser ajustadas
contas_para_ajustar = [
    '3.01', '3.02', '3.03', '3.04', '3.04.06', '3.06', '3.06.01', '3.06.02', '3.08',
    '3.09', '3.10', '3.11', '3.11.01'
]
def ajustar_trimestres_70401(grupo):
    """
    Ajusta os valores da conta 7.04.01 para calcular apenas os valores relativos a cada trimestre.
    """
    if grupo['conta'].iloc[0] != '7.04.01':
        return grupo  # Retornar o grupo sem alterações se não for a conta 7.04.01

    # Ordenar o grupo por ano e trimestre
    grupo = grupo.sort_values(by=['ano', 'trimestre'])

    # Calcular os valores relativos a cada trimestre
    valores_corrigidos = []
    acumulado_anterior = 0

    for _, row in grupo.iterrows():
        valor_atual = row['valor_primeiro_periodo']
        if row['trimestre'] == 1:
            # Para o primeiro trimestre, o valor permanece o mesmo
            valor_corrigido = valor_atual
        else:
            # Para os demais trimestres, subtrair o acumulado anterior
            valor_corrigido = valor_atual - acumulado_anterior

        valores_corrigidos.append(valor_corrigido)
        acumulado_anterior = valor_atual

    # Atualizar os valores no grupo
    grupo['valor_primeiro_periodo'] = valores_corrigidos
    return grupo

def ajustar_quarto_trimestre(grupo):
    # Verificar se a conta do grupo está na lista de contas para ajustar
    if grupo['conta'].iloc[0] not in contas_para_ajustar:
        return grupo  # Retornar o grupo sem alterações

    # Identificar o último trimestre (trimestre 4)
    ultimo_trimestre = grupo[grupo['trimestre'] == 4]
    if not ultimo_trimestre.empty:
        # Iterar sobre cada ano presente no grupo
        for ano in ultimo_trimestre['ano'].unique():
            # Filtrar os dados do respectivo ano
            grupo_ano = grupo[grupo['ano'] == ano]
            # Calcular a soma dos valores dos três primeiros trimestres do ano
            soma_primeiros_trimestres = grupo_ano[grupo_ano['trimestre'] < 4]['valor_primeiro_periodo'].sum()
            # Subtrair a soma dos três primeiros trimestres do valor acumulado do último trimestre
            grupo.loc[(grupo['trimestre'] == 4) & (grupo['ano'] == ano), 'valor_primeiro_periodo'] -= soma_primeiros_trimestres
    return grupo

# Aplicar a lógica para cada combinação de ticker e conta
df_consolidado = df_consolidado.groupby(['ticker', 'conta'], group_keys=False).apply(ajustar_quarto_trimestre)

# Aplicar abs para contas especificas
contas_abs = ['3.02', '3.04', '3.06.02', '3.08','7.08.04.01', '7.08.04.02', '7.04.01']
for conta in contas_abs:
    df_consolidado.loc[df_consolidado['conta'] == conta, 'valor_primeiro_periodo'] = df_consolidado.loc[df_consolidado['conta'] == conta, 'valor_primeiro_periodo'].abs()

# Aplicar a lógica apenas para a conta 7.04.01
df_consolidado = df_consolidado.groupby(['ticker', 'conta'], group_keys=False).apply(ajustar_trimestres_70401)

# Preencher valores ausentes com 0
df_consolidado['valor_primeiro_periodo'].fillna(0, inplace=True)

# multiplicar por 1000 todos os valore de valor_primeiro_periodo o que for negativo continua negativo
df_consolidado['valor_primeiro_periodo'] = df_consolidado['valor_primeiro_periodo'].apply(lambda x: x * 1000 if pd.notnull(x) else x)

def processar_dados_yfinance(df_consolidado):
    """
    Processa os dados de cada ticker único no DataFrame consolidado usando a API do yfinance.
    Associa corretamente a quantidade de ações (qtd_acoes) à data de envio (data_envio) usando join por intervalo.
    """
    tickers_unicos = df_consolidado['ticker'].unique()
    historico_acoes = []

    for ticker in tickers_unicos:
        try:
            print(f"Processando dados para o ticker: {ticker}")
            ticker_yf = f"{ticker}.SA"
            yf_ticker = yf.Ticker(ticker_yf)

            marketcap = yf_ticker.info.get('marketCap', None)
            preco_acao = yf_ticker.history(period="1d")['Close'].iloc[-1] if not yf_ticker.history(period="1d").empty else None

            if marketcap is not None and preco_acao is not None and preco_acao > 0:
                qtd_total_acoes = marketcap / preco_acao
            else:
                qtd_total_acoes = None

            splits = yf_ticker.splits

            if not splits.empty:
                splits = splits.sort_index(ascending=True)  # Ordenar em ordem crescente
                qtd_acoes_atual = qtd_total_acoes
                historico = []

                # Processar os splits
                for data_split, proporcao_split in splits.items():
                    if qtd_acoes_atual:
                        qtd_acoes_atual *= proporcao_split
                    data_split_limpo = pd.Timestamp(data_split).tz_localize(None).normalize()
                    historico.append({'ticker': ticker, 'data_fim': data_split_limpo, 'qtd_acoes': qtd_acoes_atual})

                # Adicionar a data atual como o registro mais recente
                data_atual = pd.Timestamp.now().tz_localize(None).normalize()
                historico.append({'ticker': ticker, 'data_fim': data_atual, 'qtd_acoes': qtd_total_acoes})

                historico_acoes.extend(historico)

            else:
                # Sem splits, mas temos o marketcap atual — registrar só a data atual
                if qtd_total_acoes:
                    data_atual = pd.Timestamp.now().tz_localize(None).normalize()
                    historico_acoes.append({'ticker': ticker, 'data_fim': data_atual, 'qtd_acoes': qtd_total_acoes})

        except Exception as e:
            print(f"Erro ao processar o ticker {ticker}: {e}", flush=True)

    # Criar DataFrame com histórico de ações
    df_historico_acoes = pd.DataFrame(historico_acoes)

    if df_historico_acoes.empty:
        print("Histórico de ações vazio. Encerrando.")
        return df_consolidado

    # Garantir normalização da data_envio
    df_consolidado = df_consolidado.copy()
    df_consolidado['data_envio'] = pd.to_datetime(df_consolidado['data_envio'], format='%d/%m/%Y').dt.normalize()
    df_historico_acoes['data_fim'] = pd.to_datetime(df_historico_acoes['data_fim']).dt.normalize()

    # Ordenar os DataFrames para uso no merge_asof
    df_consolidado = df_consolidado.sort_values(by=['ticker', 'data_envio']).reset_index(drop=True)
    df_historico_acoes = df_historico_acoes.sort_values(by=['ticker', 'data_fim']).reset_index(drop=True)

     # Realizar o merge_asof para cada grupo de ticker
    resultados = []
    for ticker, grupo_consolidado in df_consolidado.groupby('ticker'):
        grupo_historico = df_historico_acoes[df_historico_acoes['ticker'] == ticker]

        # Aplicar merge_asof no grupo
        grupo_resultado = pd.merge_asof(
            grupo_consolidado,
            grupo_historico,
            left_on='data_envio',
            right_on='data_fim',
            direction='forward'  # Busca a data mais próxima anterior ou igual
        )
        resultados.append(grupo_resultado)

    # Concatenar os resultados
    df_resultado = pd.concat(resultados, ignore_index=True)
    # Preencher os valores de 'qtd_acoes' com ffill (caso necessário)
    df_resultado = df_resultado[['data_doc','ticker_x', 'conta','valor_primeiro_periodo', 'data_envio', 'data_fim', 'qtd_acoes']]
    df_resultado.rename(columns={'ticker_x': 'ticker','qtd_acoes':'qtd_acoes_total'}, inplace=True)


    return df_resultado
    



# Chamar a função ao final do processo
df_resultado = processar_dados_yfinance(df_consolidado)





# Salvar o arquivo consolidado

resultado_file = os.path.join("dados/balancos", "balancos_consolidados.parquet")
df_ultimo_trimeste = os.path.join("dados/balancos", "balancos_consolidados12m.parquet")
df_resultado.to_parquet(resultado_file, index=False)

print(f"Arquivo consolidado salvo em {df_resultado}.")
