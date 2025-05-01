import os
import pandas as pd

def calculando_premio():

        cdi = pd.read_parquet(os.path.join(".", "dados", "acoes", "cdi.parquet"))
        cdi['cota'] = (1 + cdi['retorno']).cumprod() - 1
        ibov = pd.read_parquet(os.path.join(".", "dados", "acoes",'IBOV.parquet'))

        ibov_datas = ibov.sort_values('data', ascending = True)
        ibov_datas = ibov_datas.assign(year = pd.DatetimeIndex(ibov_datas['data']).year)
        ibov_datas = ibov_datas.assign(month = pd.DatetimeIndex(ibov_datas['data']).month)
        datas_final_mes = ibov_datas.groupby(['year', 'month'])['data'].last()
        dias_final_de_mes = datas_final_mes.to_list()

        ibov = ibov[ibov['data'].isin(dias_final_de_mes)]
        cdi = cdi[cdi['data'].isin(dias_final_de_mes)]
        ibov['retorno_ibov'] = ibov['fechamento'].pct_change()
        cdi['retorno_cdi'] = cdi['cota'].pct_change()
        ibov['data'] = ibov['data'].astype(str)
        cdi['data'] = cdi['data'].astype(str)

        df_dados_mercado = pd.merge(ibov, cdi, how = 'inner', on = "data")
        df_dados_mercado['mkt_premium'] = (1 + df_dados_mercado['retorno_ibov'])/(1 + df_dados_mercado['retorno_cdi']) - 1
        df_dados_mercado = df_dados_mercado.dropna()
        df_dados_mercado = df_dados_mercado[['data', 'mkt_premium']]
        df_dados_mercado['data'] = pd.to_datetime(df_dados_mercado['data']).dt.date

        df_dados_mercado.to_parquet(os.path.join(".", "dados", "indicadores", 'market_premium.parquet'), index = False)

def calcular_indicadores(input_path, cotacoes_path, output_path):
    """
    Calcula indicadores financeiros com base nos dados consolidados e salva os resultados.
    """
    # Caminhos
    input_file = os.path.join(input_path, "balancos_consolidados.parquet")
    cotacoes_file = os.path.join(cotacoes_path, "acoes_cotacoes.parquet")
    output_dir = os.path.join(output_path, "indicadores")
    os.makedirs(output_dir, exist_ok=True)

    # Leitura dos DataFrames
    df_balancos = pd.read_parquet(input_file)
    df_balancos.rename(columns={'data_doc': 'data'}, inplace=True)
    df_cotacoes = pd.read_parquet(cotacoes_file)

    # Conversão de datas
    df_balancos['data_envio'] = pd.to_datetime(df_balancos['data_envio'], format='%d/%m/%Y')
    df_balancos['data'] = pd.to_datetime(df_balancos['data'], format='%d/%m/%Y')
    df_cotacoes['data'] = pd.to_datetime(df_cotacoes['data'], format='%Y-%m-%d')
    
    # Renomear coluna de fechamento antes do merge
    df_cotacoes.rename(columns={'Close': 'preco'}, inplace=True)

    # Criar coluna auxiliar para merge
    df_balancos['data_join'] = df_balancos['data_envio']

    # Ordenar DataFrames para merge_asof
    df_cotacoes.sort_values(by='data', inplace=True)
    df_balancos.sort_values(by='data_join', inplace=True)

    # Merge aproximado pelas datas úteis e ticker
    df = pd.merge_asof(
        df_balancos,
        df_cotacoes,
        left_on='data_join',
        right_on='data',
        by='ticker',
        direction='backward'
    )
    
    df = df.dropna()
    
    df.rename(columns={'data_x': 'data'}, inplace=True)

    def expandir_intervalos(df_indicador):
        # Lista para armazenar os DataFrames expandidos
        dfs_expandidos = []

        # Iterar por cada ticker
        for ticker, grupo in df_indicador.groupby('ticker'):
            # Ordenar o grupo por data_envio
            grupo = grupo.sort_values('data_envio')

            # Lista para armazenar os registros expandidos do grupo
            registros_expandidos = []

            # Iterar por cada linha do grupo
            for i in range(len(grupo) - 1):
                linha_atual = grupo.iloc[i]
                linha_proxima = grupo.iloc[i + 1]

                # Gerar intervalo de datas diárias
                intervalo_datas = pd.date_range(start=linha_atual['data_envio'], 
                                                end=linha_proxima['data_envio'] - pd.Timedelta(days=1), 
                                                freq='D')

                # Criar registros para o intervalo
                for data in intervalo_datas:
                    registros_expandidos.append({
                        'data': linha_atual['data'],
                        'data_envio': data,
                        'ticker': linha_atual['ticker'],
                        'valor': linha_atual['valor']
                    })

            # Adicionar o último intervalo do grupo
            ultima_linha = grupo.iloc[-1]
            intervalo_final = pd.date_range(start=ultima_linha['data_envio'], 
                                            end=ultima_linha['data_envio'] + pd.Timedelta(days=1), 
                                            freq='D')
            for data in intervalo_final:
                registros_expandidos.append({
                    'data': ultima_linha['data'],
                    'data_envio': data,
                    'ticker': ultima_linha['ticker'],
                    'valor': ultima_linha['valor']
                })

            # Criar DataFrame expandido para o grupo
            df_expandido = pd.DataFrame(registros_expandidos)
            dfs_expandidos.append(df_expandido)

        # Concatenar todos os DataFrames expandidos
        df_resultado = pd.concat(dfs_expandidos, ignore_index=True)
        return df_resultado
    # Função para salvar indicadores
    def salvar_indicador(df_indicador, nome_indicador):
        # Renomear a coluna do indicador para valor
        df_indicador.rename(columns={nome_indicador: 'valor'}, inplace=True)
        
        df_indicador = expandir_intervalos(df_indicador)
        df_indicador = df_indicador[['data_envio','ticker','valor']]
        df_indicador.rename(columns={'data_envio': 'data'}, inplace=True)
        
        output_file = os.path.join(output_dir, f"{nome_indicador}.parquet")
        df_indicador.to_parquet(output_file, index=False)
        print(f"Indicador salvo: {output_file}")

    # Cálculo dos indicadores
    indicadores = pd.DataFrame()  # Inicializa como DataFrame vazio
    indicadores = df[['data', 'data_envio', 'ticker','close', 'close_hist', 'qtd_acoes_total']].drop_duplicates().copy()

    ############### Valor de Mercado

    # Verifica se as colunas necessárias estão disponíveis
    if 'close_hist' not in indicadores.columns or 'qtd_acoes_total' not in indicadores.columns:
        print("Aviso: Dados insuficientes para calcular o Valor de Mercado. Continuando com valores nulos.")
        indicadores['ValorMercado'] = None
    else:
        # Calcula o Valor de Mercado
        indicadores['ValorMercado'] = indicadores['close_hist'] * indicadores['qtd_acoes_total']

        # Trata valores nulos ou infinitos
        indicadores['ValorMercado'] = indicadores['ValorMercado'].replace([float('inf'), -float('inf')], None)

    ############### EBIT
    df_ebit = df[df['conta'].isin(['3.03', '3.04'])].groupby(
        ['data', 'data_envio', 'ticker'], as_index=False
    ).apply(
        lambda x: pd.Series({
            'EBIT': x.loc[x['conta'] == '3.03', 'valor_primeiro_periodo'].sum() -
                    x.loc[x['conta'] == '3.04', 'valor_primeiro_periodo'].sum()
        })
    ).reset_index(drop=True)  # Reseta os índices para evitar problemas no merge

    
    
    
    ############### EBITDA
    # Verifica se df_ebit está vazio
    if df_ebit.empty:
        raise ValueError("O DataFrame 'EBIT' está vazio. Verifique os cálculos anteriores.")

    # Junta o EBIT ao DataFrame de indicadores
    indicadores = indicadores.merge(df_ebit, on=['data', 'data_envio', 'ticker'], how='left')

    ## Depreciação
    df_depreciacao = df[df['conta'] == '7.04.01'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]

    if df_depreciacao.empty:
        print("Aviso: O DataFrame 'Depreciação' está vazio. Continuando com valores nulos para EBITDA.")
        indicadores['EBITDA'] = indicadores['EBIT']  # EBITDA será igual ao EBIT
    else:
        # Junta a depreciação ao DataFrame de indicadores
        indicadores = indicadores.merge(
            df_depreciacao.rename(columns={'valor_primeiro_periodo': 'Depreciacao'}),
            on=['data', 'data_envio', 'ticker'],
            how='left'
        )
        # Calcula o EBITDA
        indicadores['EBITDA'] = indicadores['EBIT'] + indicadores['Depreciacao'].fillna(0)



    ############### EBIT / Ativos
    # Filtrar o Ativo Total (conta 1)
    df_ativo_total = df[df['conta'] == '1'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    df_ativo_total = df_ativo_total.rename(columns={'valor_primeiro_periodo': 'AtivoTotal'})

    # Verifica se df_ebit está vazio
    if df_ebit.empty:
        raise ValueError("O DataFrame 'EBIT' está vazio. Verifique os cálculos anteriores.")

    # Verifica se df_ativo_total está vazio
    if df_ativo_total.empty:
        print("Aviso: O DataFrame 'Ativo Total' está vazio. Continuando com valores nulos para o indicador EBIT / Ativos.")
        indicadores['EBIT_Ativos'] = None
    else:
        # Junta o Ativo Total ao DataFrame de indicadores
        indicadores = indicadores.merge(df_ativo_total, on=['data', 'data_envio', 'ticker'], how='left')

        # Calcula o indicador EBIT / Ativos
        indicadores['EBIT_Ativos'] = indicadores['EBIT'] / indicadores['AtivoTotal']

        # Trata divisões por zero ou valores nulos
        indicadores['EBIT_Ativos'] = indicadores['EBIT_Ativos'].replace([float('inf'), -float('inf')], None)



    ############### EBIT / Despesas Financeiras
    # Filtras Despesas financerias (conta 3.06.02)
    df_despesas_financeiras = df[df['conta'] == '3.06.02'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    df_despesas_financeiras = df_despesas_financeiras.rename(columns={'valor_primeiro_periodo':'DespesasFinanceiras'})
    
    # Verifica se df_ebit está vazio
    if df_ebit.empty:
        raise ValueError("O DataFrame 'EBIT' está vazio. Verifique os cálculos anteriores.")

    # Verifica se df_despesas_financeiras está vazio
    if df_despesas_financeiras.empty:
        print("Aviso: O DataFrame 'Ativo Total' está vazio. Continuando com valores nulos para o indicador EBIT / Despesas Financeiras.")
        indicadores['EBIT_DespesasFinanceiras'] = None
    else:
        # Junta o Ativo Total ao DataFrame de indicadores
        indicadores = indicadores.merge(df_despesas_financeiras, on=['data', 'data_envio', 'ticker'], how='left')

        # Calcula o indicador EBIT / Ativos
        indicadores['EBIT_DespesasFinanceiras'] = indicadores['EBIT'] / indicadores['DespesasFinanceiras']
        
        # Calcula o indicador EBITDA_DespesasFinanceiras
        indicadores['EBITDA_DespesasFinanceiras'] = indicadores['EBITDA'] / indicadores['DespesasFinanceiras']
        
        # Calcula o LAIR 
        indicadores['LAIR'] = indicadores['EBIT'] - indicadores['DespesasFinanceiras']

        # Trata divisões por zero ou valores nulos
        indicadores['EBIT_DespesasFinanceiras'] = indicadores['EBIT_DespesasFinanceiras'].replace([float('inf'), -float('inf')], None)
        indicadores['EBITDA_DespesasFinanceiras'] = indicadores['EBITDA_DespesasFinanceiras'].replace([float('inf'), -float('inf')], None)


    ############## Dívida Líquida / EBIT e EBITDA e PatrimonioLiquido e EBIT/DividaLiquida E Enteprise Value (EV)
    df_divida_liquida = df[df['conta'].isin(['2.01.04', '2.02.01', '1.01.01'])].groupby(
        ['data', 'data_envio', 'ticker'], as_index=False
    ).apply(
        lambda x: pd.Series({
            'DividaLiquida': x.loc[x['conta'].isin(['2.01.04', '2.02.01']), 'valor_primeiro_periodo'].sum() -
                            x.loc[x['conta'] == '1.01.01', 'valor_primeiro_periodo'].sum()
        })
    ).reset_index(drop=True)  # Reseta os índices para evitar problemas no merge
    
    
        
    # Filtrar o Patrimônio Líquido (conta 2.03)
    df_patrimonio_liquido = df[df['conta'] == '2.03'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    df_patrimonio_liquido = df_patrimonio_liquido.rename(columns={'valor_primeiro_periodo': 'PatrimonioLiquido'})

    # Verifica se df_divida_liquida está vazio
    if df_divida_liquida.empty or df_patrimonio_liquido.empty:
        print("Aviso: O DataFrame 'Dívida Líquida' está vazio. Continuando com valores nulos para DividaLiquida_EBIT.")
        indicadores['DividaLiquida_EBIT'] = None
        indicadores['DividaLiquida_EBITDA'] = None
        indicadores['PatrimonioLiquido'] = None
    else:
        # Junta a Dívida Líquida ao DataFrame de indicadores
        indicadores = indicadores.merge(df_divida_liquida, on=['data', 'data_envio', 'ticker'], how='left')
        indicadores = indicadores.merge(df_patrimonio_liquido, on=['data', 'data_envio', 'ticker'], how='left')

        # Calcula DividaLiquida_EBIT
        indicadores['DividaLiquida_EBIT'] = indicadores['DividaLiquida'] / indicadores['EBIT']
        
        # Calula EBIT_DividaLiquida
        indicadores['ebit_dl'] = indicadores.apply(
            lambda row: 999 if row['DividaLiquida'] <= 0 else 
                        -999 if row['EBIT'] <= 0 else 
                        row['EBIT'] / row['DividaLiquida'], 
            axis=1
        )
        
        # Calcula DividaLiquida_EBITDA
        indicadores['DividaLiquida_EBITDA'] = indicadores['DividaLiquida'] / indicadores['EBITDA']

        # Calcula DividaLiquida_PatrimonioLiquido
        indicadores['DividaLiquida_PatrimonioLiquido'] = indicadores['DividaLiquida'] / indicadores['PatrimonioLiquido']
        
        # Calcula Enterprise Value (EV)
        indicadores['EV'] = indicadores['DividaLiquida'] + indicadores['ValorMercado']
        
        # Calcula o EBIT_EV
        indicadores['EBIT_EV'] = indicadores['EBIT'] / indicadores['EV']
        
        # Calcula o EBITA_EV
        indicadores['EBITDA_EV'] = indicadores['EBITDA'] / indicadores['EV']
        
        # Calcula o EV_EBIT
        indicadores['EV_EBIT'] = indicadores['EV'] / indicadores['EBIT']
        
        # Calcula o EV_EBITA
        indicadores['EV_EBITA'] = indicadores['EV'] / indicadores['EBITDA']
        
        # Calcula o PatrimonioLiquido_Ativos
        indicadores['PatrimonioLiquido_Ativos'] =  indicadores['PatrimonioLiquido'] / indicadores['AtivoTotal']
        

        # Trata divisões por zero ou valores nulos
        indicadores['DividaLiquida_EBIT'] = indicadores['DividaLiquida_EBIT'].replace([float('inf'), -float('inf')], None)
        indicadores['DividaLiquida_EBITDA'] = indicadores['DividaLiquida_EBITDA'].replace([float('inf'), -float('inf')], None)
        indicadores['DividaLiquida_PatrimonioLiquido'] = indicadores['DividaLiquida_PatrimonioLiquido'].replace([float('inf'), -float('inf')], None)
        indicadores['ebit_dl'] = indicadores['ebit_dl'].replace([float('inf'), -float('inf')], None)
        indicadores['EV'] = indicadores['EV'].replace([float('inf'), -float('inf')], None)
        indicadores['EBIT_EV'] = indicadores['EBIT_EV'].replace([float('inf'), -float('inf')], None)
        indicadores['EBITDA_EV'] = indicadores['EBITDA_EV'].replace([float('inf'), -float('inf')], None)
        indicadores['EV_EBIT'] = indicadores['EV_EBIT'].replace([float('inf'), -float('inf')], None)
        indicadores['EV_EBITA'] = indicadores['EV_EBITA'].replace([float('inf'), -float('inf')], None)
        indicadores['PatrimonioLiquido_Ativos'] = indicadores['PatrimonioLiquido_Ativos'].replace([float('inf'), -float('inf')], None)
        
        #indicadores['DividaLiquida_EBIT'] = indicadores['DividaLiquida_EBIT'].fillna(0)
        

    ############# Margem Bruta
    df_resultado_bruto = df[df['conta'] == '3.03'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    df_receita_liquida = df[df['conta'] == '3.01'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]

    if df_resultado_bruto.empty or df_receita_liquida.empty:
        print("Aviso: Dados insuficientes para calcular Margem Bruta. Continuando com valores nulos.")
        indicadores['MargemBruta'] = None
        indicadores['MargemEBIT'] = None
    else:
        # Junta os DataFrames de Resultado Bruto e Receita Líquida
        df_margem_bruta = pd.merge(
            df_resultado_bruto.rename(columns={'valor_primeiro_periodo': 'ResultadoBruto'}),
            df_receita_liquida.rename(columns={'valor_primeiro_periodo': 'ReceitaLiquida'}),
            on=['data', 'data_envio', 'ticker'],
            how='left'
        )
        # Calcula Margem Bruta
        df_margem_bruta['MargemBruta'] = df_margem_bruta['ResultadoBruto'] / df_margem_bruta['ReceitaLiquida']
        indicadores = indicadores.merge(
            df_margem_bruta[['data', 'data_envio', 'ticker', 'MargemBruta']],
            on=['data', 'data_envio', 'ticker'],
            how='left'
        )



    ############# Margem EBIT, Margem EBITDA e Margem Líquida

    # Filtrar Receita Líquida (conta 3.01)
    df_receita_liquida = df[df['conta'] == '3.01'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    df_receita_liquida.rename(columns={'valor_primeiro_periodo': 'ReceitaLiquida'}, inplace=True)

    # Verifica se Receita Líquida está vazia
    if df_receita_liquida.empty:
        print("Aviso: Dados insuficientes para calcular margens. Continuando com valores nulos.")
        indicadores['MargemEBIT'] = None
        indicadores['MargemEBITDA'] = None
        indicadores['MargemLiquida'] = None
    else:
        # Junta Receita Líquida ao DataFrame de indicadores
        indicadores = indicadores.merge(df_receita_liquida, on=['data', 'data_envio', 'ticker'], how='left')

        # Calcula Margem EBIT
        indicadores['MargemEBIT'] = indicadores['EBIT'] / indicadores['ReceitaLiquida']

        # Calcula Margem EBITDA
        indicadores['MargemEBITDA'] = indicadores['EBITDA'] / indicadores['ReceitaLiquida']

        # Filtrar Lucro Líquido (conta 3.11)
        df_lucro_liquido = df[df['conta'] == '3.11'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
        df_lucro_liquido.rename(columns={'valor_primeiro_periodo': 'LucroLiquido'}, inplace=True)

        if df_lucro_liquido.empty:
            print("Aviso: Dados insuficientes para calcular Margem Líquida. Continuando com valores nulos.")
            indicadores['MargemLiquida'] = None
        else:
            # Junta Lucro Líquido ao DataFrame de indicadores
            indicadores = indicadores.merge(df_lucro_liquido, on=['data', 'data_envio', 'ticker'], how='left')

            # Calcula Margem Líquida
            indicadores['MargemLiquida'] = indicadores['LucroLiquido'] / indicadores['ReceitaLiquida']

        # Trata divisões por zero ou valores nulos
        indicadores['MargemEBIT'] = indicadores['MargemEBIT'].replace([float('inf'), -float('inf')], None)
        indicadores['MargemEBITDA'] = indicadores['MargemEBITDA'].replace([float('inf'), -float('inf')], None)
        indicadores['MargemLiquida'] = indicadores['MargemLiquida'].replace([float('inf'), -float('inf')], None)

    ############### ROE, ROA e ROIC e Lucro/Preço (L_P) , P_L , P_EBITDA e P_EBIT

    # Filtrar Patrimônio Líquido e Passivo Circulante
    df_passivo_circulante = df[df['conta'] == '2.01'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]

    if df_patrimonio_liquido.empty or df_passivo_circulante.empty or df_lucro_liquido.empty or 'close_hist' not in indicadores.columns:
        print("Aviso: Dados insuficientes para calcular ROE, ROA e ROIC. Continuando com valores nulos.")
        indicadores['ROE'] = None
        indicadores['ROA'] = None
        indicadores['ROIC'] = None
        indicadores['L_P'] = None
        indicadores['VPA'] = None
        indicadores['P_L'] = None
        indicadores['P_EBIT'] = None
        indicadores['P_EBITDA'] = None
        
    else:
        # Junta os DataFrames necessários ao DataFrame de indicadores
        indicadores = indicadores.merge(
            df_passivo_circulante.rename(columns={'valor_primeiro_periodo': 'PassivoCirculante'}),
            on=['data', 'data_envio', 'ticker'],
            how='left'
        )
        # Calcula o Lucro por Ação
        indicadores['LucroPorAcao'] = (indicadores['LucroLiquido']) * 4 / indicadores['qtd_acoes_total']
        
        indicadores['P_L'] = indicadores['close_hist'] / indicadores['LucroPorAcao']
        
        # Calcula Lucro/Preço (L/P)
        indicadores['L_P'] = indicadores['LucroPorAcao'] / indicadores['close_hist']
        
        # Calcula Preço/Lucro (P/L) 
        
        # Calcula Preço/Ebitda (P/EBITDA)
        indicadores['P_EBIT'] = indicadores['close_hist'] / indicadores['EBIT']
        
        # Calcula Preço/Ebitda (P/EBITDA)
        indicadores['P_EBITDA'] = indicadores['close_hist'] / indicadores['EBITDA']

        # Calcula ROE
        indicadores['ROE'] = (indicadores['LucroLiquido']) * 4 / indicadores['PatrimonioLiquido']

        # Calcula ROA (AtivoTotal já existe em indicadores)
        indicadores['ROA'] = (indicadores['LucroLiquido'] / indicadores['AtivoTotal']) * 4

        # Calcula ROIC
        indicadores['ROIC'] = (indicadores['LucroLiquido']) * 4 / (indicadores['AtivoTotal'] - indicadores['PassivoCirculante'])

        # Trata divisões por zero ou valores nulos
        indicadores['ROE'] = indicadores['ROE'].replace([float('inf'), -float('inf')], None)
        indicadores['ROA'] = indicadores['ROA'].replace([float('inf'), -float('inf')], None)
        indicadores['ROIC'] = indicadores['ROIC'].replace([float('inf'), -float('inf')], None)
        indicadores['L_P'] = indicadores['L_P'].replace([float('inf'), -float('inf')], None)    
        indicadores['P_L'] = indicadores['P_L'].replace([float('inf'), -float('inf')], None)
        indicadores['P_EBIT'] = indicadores['P_EBIT'].replace([float('inf'), -float('inf')], None)
        indicadores['P_EBITDA'] = indicadores['P_EBITDA'].replace([float('inf'), -float('inf')], None)
                    
      
    ############# Cálculo do Capital de Giro e P/Capital de Giro +  P/Ativo Circulante Líquido

    # Filtrar Ativo Circulante (conta 1.01)
    df_ativo_circulante = df[df['conta'] == '1.01'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    df_ativo_circulante.rename(columns={'valor_primeiro_periodo': 'AtivoCirculante'}, inplace=True)
    
    # Filtrar Passivo Não Circulante (conta 2.02)
    df_passivo_nao_circulante = df[df['conta'] == '2.02'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    df_passivo_nao_circulante.rename(columns={'valor_primeiro_periodo': 'PassivoNaoCirculante'}, inplace=True)

    # Verifica se Ativo Circulante e Passivo Circulante estão disponíveis
    if df_ativo_circulante.empty or df_passivo_circulante.empty or df_passivo_nao_circulante.empty:
        print("Aviso: Dados insuficientes para calcular Capital de Giro. Continuando com valores nulos.")
        indicadores['CapitalDeGiro'] = None
        indicadores['P_Ativo'] = None
        indicadores['P_AtivoCirculanteLiquido'] = None
    else:
        # Junta Ativo Circulante e Passivo Circulante ao DataFrame de indicadores
        indicadores = indicadores.merge(df_ativo_circulante, on=['data', 'data_envio', 'ticker'], how='left')
        indicadores = indicadores.merge(df_passivo_nao_circulante, on=['data', 'data_envio', 'ticker'], how='left')

        # Calcula o Ativo Circulante Líquido (ACL)
        indicadores['AtivoCirculanteLiquido'] = (
            indicadores['AtivoCirculante'] - 
            (indicadores['PassivoCirculante'] + indicadores['PassivoNaoCirculante'])
        )

        # Calcula o Ativo Circulante Líquido por Ação
        indicadores['ACL_PorAcao'] = indicadores['AtivoCirculanteLiquido'] / indicadores['qtd_acoes_total']

        # Calcula o indicador P/Ativo Circulante Líquido
        indicadores['P_AtivoCirculanteLiquido'] = indicadores['close_hist'] / indicadores['ACL_PorAcao']

        # Calcula Circulante liquido
        indicadores['CapitalDeGiro'] = indicadores['AtivoCirculante'] - indicadores['PassivoCirculante']

        # Calcula P/Circulante liquido
        indicadores['P_CapitalDeGiro'] = indicadores['close_hist'] / (indicadores['CapitalDeGiro'] / indicadores['qtd_acoes_total'])
        
        # Calcula P_Ativos
        indicadores['P_Ativos'] = indicadores['close_hist'] / (indicadores['AtivoTotal'] / indicadores['qtd_acoes_total'])

        # Trata divisões por zero ou valores nulos
        indicadores['CapitalDeGiro'] = indicadores['CapitalDeGiro'].replace([float('inf'), -float('inf')], None)
        indicadores['P_CapitalDeGiro'] = indicadores['P_CapitalDeGiro'].replace([float('inf'), -float('inf')], None)
        indicadores['P_Ativos'] = indicadores['P_Ativos'].replace([float('inf'), -float('inf')], None)
        indicadores['P_AtivoCirculanteLiquido'] = indicadores['P_AtivoCirculanteLiquido'].replace([float('inf'), -float('inf')], None)
        
        
    ############### VPA (Valor Patrimonial por Ação) e P/VPA (Preço/Valor Patrimonial por Ação)
    
    # Verifica se os DataFrames necessários estão disponíveis
    if df_patrimonio_liquido.empty:
        print("Aviso: Dados insuficientes para calcular VPA e P/VPA. Continuando com valores nulos.")
        indicadores['VPA'] = None
        indicadores['P_VPA'] = None
        indicadores['PSR'] = None
    else:
        # Calcula a Receita Líquida por Ação
        indicadores['ReceitaLiquidaPorAcao'] = indicadores['ReceitaLiquida'] / indicadores['qtd_acoes_total']

        # Calcula o PSR (Preço sobre Receita)
        indicadores['PSR'] = indicadores['close_hist'] / indicadores['ReceitaLiquidaPorAcao']

        # Calcula o VPA (Valor Patrimonial por Ação)
        indicadores['VPA'] = indicadores['PatrimonioLiquido'] / indicadores['qtd_acoes_total']

        # Calcula o P/VPA (Preço/Valor Patrimonial por Ação)
        indicadores['P_VPA'] = indicadores['close_hist'] / indicadores['VPA']

        # Trata divisões por zero ou valores nulos
        indicadores['VPA'] = indicadores['VPA'].replace([float('inf'), -float('inf')], None)
        indicadores['P_VPA'] = indicadores['P_VPA'].replace([float('inf'), -float('inf')], None)
        indicadores['ReceitaLiquidaPorAcao'] = indicadores['ReceitaLiquidaPorAcao'].replace([float('inf'), -float('inf')], None)
        indicadores['PSR'] = indicadores['PSR'].replace([float('inf'), -float('inf')], None)
        
    
    
    ############# Cálculo do Dividend Yield (DY)

    # Filtrar Dividendos pagos a acionistas controladores (6.03.05) e não controladores (6.03.06)
    df_dividendos = df[df['conta'].isin(['7.08.04.01', '7.08.04.02'])][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]

    # Verificar se há dados de dividendos
    if df_dividendos.empty:
        print("Aviso: Dados insuficientes para calcular Dividend Yield (DY). Continuando com valores nulos.")
        indicadores['DividendYield'] = None
    else:
        # Tornar os valores positivos (valor absoluto)
        df_dividendos['valor_primeiro_periodo'] = df_dividendos['valor_primeiro_periodo'].abs()

        # Agrupar por data, data_envio e ticker para somar os dividendos
        df_dividendos_agrupados = df_dividendos.groupby(['data', 'data_envio', 'ticker'], as_index=False)['valor_primeiro_periodo'].sum()
        df_dividendos_agrupados.rename(columns={'valor_primeiro_periodo': 'DividendosTotais'}, inplace=True)

        # Verificar se a quantidade total de ações está disponível
        if 'qtd_acoes_total' not in indicadores.columns:
            print("Aviso: Quantidade total de ações não disponível. Não é possível calcular Dividendos por Ação.")
            indicadores['DividendYield'] = None
        else:
            # Verificar se o preço da ação (close_hist) está disponível
            if 'close_hist' not in indicadores.columns:
                print("Aviso: Preço da Ação (close_hist) não disponível. Não é possível calcular o Dividend Yield (DY).")
                indicadores['DividendYield'] = None
            else:
                # Mesclar os dividendos calculados ao DataFrame de indicadores
                indicadores = indicadores.merge(df_dividendos_agrupados, on=['data', 'data_envio', 'ticker'], how='left')
                
                indicadores['DividendosPorAcao'] = indicadores['DividendosTotais'] / indicadores['qtd_acoes_total']

                # Calcular o Dividend Yield (DY)
                indicadores['DividendYield'] = (indicadores['DividendosPorAcao'] / indicadores['close_hist'])

                # Tratar divisões por zero ou valores nulos
                indicadores['DividendYield'] = indicadores['DividendYield'].replace([float('inf'), -float('inf')], None)


    # Excluir colunas indesejadas
    colunas_excluir = ['close', 'qtd_acoes_total', 'close_hist']
    indicadores = indicadores.drop(columns=colunas_excluir, errors='ignore')        
        
    calculando_premio()
    # Salvar todos os indicadores, exceto 'data', 'data_envio', 'ticker'
    for coluna in indicadores.columns:
        if coluna not in ['data','data_envio', 'ticker']:
            salvar_indicador(indicadores[['data', 'data_envio', 'ticker', coluna]], coluna)
        

def separar_itens_contabeis(input_path, dados_path):
    """
    Lê o arquivo balancos_consolidados.parquet e separa os itens contábeis BP e DRE/DFC em arquivos individuais.
    """
    # Caminhos
    input_file = os.path.join(input_path, "balancos_consolidados.parquet")
    output_dir_bp = os.path.join(dados_path, "contabeis_bp")
    output_dir_dre_dfc = os.path.join(dados_path, "contabeis_dre_e_dfc")
    os.makedirs(output_dir_bp, exist_ok=True)
    os.makedirs(output_dir_dre_dfc, exist_ok=True)

    # Indicadores BP
    indicadores_bp = {
        'AtivoCirculante': '1.01',
        'AtivoNaoCirculante': '1.02',
        'AtivoTotal': '1',
        'CaixaEquivalentes': '1.01.01',
        'DespesasFinanceiras': '3.06.02',
        'Disponibilidades': '1.01.01',
        'DividaBruta_PassivoCirculante': '2.01.04',
        'DividaBruta_PassivoNaoCirculante': '2.02.01',
        'DividaLiquida': 'DividaLiquida',
        #'EBITDA': 'EBITDA',
        'PassivoCirculante': '2.01',
        'PassivoNaoCirculante': '2.02',
        'PassivoTotal': '2',
        'PatrimonioLiquido': '2.03',
    }

    # Indicadores DRE/DFC
    indicadores_dre_dfc = {
        'ReceitaLiquida': '3.01',
        'Custos': '3.02',
        'ResultadoBruto': '3.03',
        'DespesasReceitasOperacionaisOuAdministrativas': '3.04',
        'EBIT': 'EBIT',
        'ResultadoFinanceiro': '3.06',
        'ReceitasFinanceiras': '3.06.01',
        'LAIR': 'LAIR',
        'Impostos': '3.08',
        'LucroLiquidoOperacoesContinuadas': '3.09',
        'LucroLiquidoOperacoesDescontinuadas': '3.10',
        'LucroLiquido': '3.11',
        'LucroLiquidoSociosControladora': '3.11.01',
        'JCP':'7.08.04.01',
        'Dividendos':'7.08.04.02',
        'DepreciacaoAmortizacao': '7.04.01',
        'EquivalenciaPatrimonial': '3.04.06',
    }

    # Lê o DataFrame
    df = pd.read_parquet(input_file)

    # Função para processar e salvar os indicadores
    def processar_e_salvar(indicadores, output_dir):
        for nome, conta in indicadores.items():
            if conta in df['conta'].values or conta == 'DividaLiquida':
                if conta == 'DividaLiquida':
                    # Calcula Dívida Líquida: Dívida Bruta - Caixa e Equivalentes
                    df_divida_bruta = df[df['conta'].isin(['2.01.04', '2.02.01'])].groupby(
                        ['data', 'data_envio', 'ticker'], as_index=False)['valor_primeiro_periodo'].sum()
                    df_caixa = df[df['conta'] == '1.01.01'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
                    df_item = pd.merge(df_divida_bruta, df_caixa, on=['data', 'data_envio', 'ticker'], how='left')
                    df_item['valor_primeiro_periodo'] = df_item['valor_primeiro_periodo_x'] - df_item['valor_primeiro_periodo_y']
                    df_item = df_item[['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
                else:
                    # Filtra os dados com base na conta
                    df_item = df[df['conta'] == conta][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
                    # Renomear coluna valor
                    df_item.rename(columns={'valor_primeiro_periodo': 'valor'}, inplace=True)

                # Salva o DataFrame em um arquivo .parquet
                output_file = os.path.join(output_dir, f"{nome}.parquet")
                df_item.to_parquet(output_file, index=False)
                print(f"Arquivo salvo: {output_file}")
            else:
                print(f"Conta {conta} não encontrada no DataFrame.")

    # Processa e salva os indicadores BP
    processar_e_salvar(indicadores_bp, output_dir_bp)

    # Processa e salva os indicadores DRE/DFC
    processar_e_salvar(indicadores_dre_dfc, output_dir_dre_dfc)

# Exemplo de uso
if __name__ == "__main__":
    input_path = "dados/balancos"
    cotacoes_path = "dados/acoes"
    output_path = "dados"
    #calcular_indicadores(input_path, cotacoes_path, output_path)
    calculando_premio()