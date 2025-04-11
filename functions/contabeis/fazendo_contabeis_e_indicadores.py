import os
import pandas as pd

def calcular_indicadores(input_path, cotacoes_path, output_path):
    """
    Calcula indicadores financeiros com base nos dados consolidados e salva os resultados.
    """
    # Caminhos
    input_file = os.path.join(input_path, "balancos_consolidados.parquet")
    cotacoes_file = os.path.join(cotacoes_path, "acoes_cotacoes.parquet")
    output_dir = os.path.join(output_path, "indicadores")
    os.makedirs(output_dir, exist_ok=True)

    # Lê os DataFrames
    df_balancos = pd.read_parquet(input_file)
    df_balancos.rename(columns={'data_doc': 'data'}, inplace=True)
    df_cotacoes = pd.read_parquet(cotacoes_file)
    df_balancos['data'] = pd.to_datetime(df_balancos['data'], format='%d/%m/%Y')
    df_cotacoes['data'] = pd.to_datetime(df_cotacoes['data'], format='%Y-%m-%d')

    # Renomear coluna de preço de fechamento
    df_cotacoes.rename(columns={'Close': 'preco'}, inplace=True)

    # Mesclar os dados de balanços com as cotações
    df = pd.merge(df_balancos, df_cotacoes, on=['data', 'ticker'], how='left')

    # Função para salvar indicadores
    def salvar_indicador(df_indicador, nome_indicador):
        # Renomear a coluna do indicador para valor
        df_indicador.rename(columns={nome_indicador: 'valor'}, inplace=True)
        
        output_file = os.path.join(output_dir, f"{nome_indicador}.parquet")
        df_indicador.to_parquet(output_file, index=False)
        print(f"Indicador salvo: {output_file}")

    # Cálculo dos indicadores
    indicadores = pd.DataFrame()  # Inicializa como DataFrame vazio
    indicadores = df[['data', 'data_envio', 'ticker']].drop_duplicates().copy()


    # EBIT
    df_ebit = df[df['conta'].isin(['3.03', '3.04'])].groupby(
        ['data', 'data_envio', 'ticker'], as_index=False
    ).apply(
        lambda x: pd.Series({
            'EBIT': x.loc[x['conta'] == '3.03', 'valor_primeiro_periodo'].sum() -
                    x.loc[x['conta'] == '3.04', 'valor_primeiro_periodo'].sum()
        })
    ).reset_index(drop=True)  # Reseta os índices para evitar problemas no merge

    # EBITDA
    # Verifica se df_ebit está vazio
    if df_ebit.empty:
        raise ValueError("O DataFrame 'EBIT' está vazio. Verifique os cálculos anteriores.")

    # Junta o EBIT ao DataFrame de indicadores
    indicadores = indicadores.merge(df_ebit, on=['data', 'data_envio', 'ticker'], how='left')

    # Depreciação
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

    # Dívida Líquida / EBIT
        df_divida_liquida = df[df['conta'].isin(['2.01.04', '2.02.01', '1.01.01'])].groupby(
        ['data', 'data_envio', 'ticker'], as_index=False
    ).apply(
        lambda x: pd.Series({
            'DividaLiquida': x.loc[x['conta'].isin(['2.01.04', '2.02.01']), 'valor_primeiro_periodo'].sum() -
                            x.loc[x['conta'] == '1.01.01', 'valor_primeiro_periodo'].sum()
        })
    ).reset_index(drop=True)  # Reseta os índices para evitar problemas no merge

    # Verifica se df_divida_liquida está vazio
    if df_divida_liquida.empty:
        print("Aviso: O DataFrame 'Dívida Líquida' está vazio. Continuando com valores nulos para DividaLiquida_EBIT.")
        indicadores['DividaLiquida_EBIT'] = None
    else:
        # Junta a Dívida Líquida ao DataFrame de indicadores
        indicadores = indicadores.merge(df_divida_liquida, on=['data', 'data_envio', 'ticker'], how='left')

        # Calcula DividaLiquida_EBIT
        indicadores['DividaLiquida_EBIT'] = indicadores['DividaLiquida'] / indicadores['EBIT']

        # Trata divisões por zero ou valores nulos
        indicadores['DividaLiquida_EBIT'] = indicadores['DividaLiquida_EBIT'].replace([float('inf'), -float('inf')], None)
        #indicadores['DividaLiquida_EBIT'] = indicadores['DividaLiquida_EBIT'].fillna(0)

    # Margem Bruta
    df_resultado_bruto = df[df['conta'] == '3.03'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    df_receita_liquida = df[df['conta'] == '3.01'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]

    if df_resultado_bruto.empty or df_receita_liquida.empty:
        print("Aviso: Dados insuficientes para calcular Margem Bruta. Continuando com valores nulos.")
        indicadores['MargemBruta'] = None
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

    # Margem Líquida
    df_lucro_liquido = df[df['conta'] == '3.11'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]

    if df_lucro_liquido.empty or df_receita_liquida.empty:
        print("Aviso: Dados insuficientes para calcular Margem Líquida. Continuando com valores nulos.")
        indicadores['MargemLiquida'] = None
    else:
        # Junta os DataFrames de Lucro Líquido e Receita Líquida
        df_margem_liquida = pd.merge(
            df_lucro_liquido.rename(columns={'valor_primeiro_periodo': 'LucroLiquido'}),
            df_receita_liquida.rename(columns={'valor_primeiro_periodo': 'ReceitaLiquida'}),
            on=['data', 'data_envio', 'ticker'],
            how='left'
        )
        # Calcula Margem Líquida
        df_margem_liquida['MargemLiquida'] = df_margem_liquida['LucroLiquido'] / df_margem_liquida['ReceitaLiquida']
        indicadores = indicadores.merge(
            df_margem_liquida[['data', 'data_envio', 'ticker', 'MargemLiquida']],
            on=['data', 'data_envio', 'ticker'],
            how='left'
        )

    # ROE
    df_patrimonio_liquido = df[df['conta'] == '2.03'][['data', 'data_envio', 'ticker', 'valor_primeiro_periodo']]

    if df_lucro_liquido.empty or df_patrimonio_liquido.empty:
        print("Aviso: Dados insuficientes para calcular ROE. Continuando com valores nulos.")
        indicadores['ROE'] = None
    else:
        # Junta os DataFrames de Lucro Líquido e Patrimônio Líquido
        df_roe = pd.merge(
            df_lucro_liquido.rename(columns={'valor_primeiro_periodo': 'LucroLiquido'}),
            df_patrimonio_liquido.rename(columns={'valor_primeiro_periodo': 'PatrimonioLiquido'}),
            on=['data', 'data_envio', 'ticker'],
            how='left'
        )
        # Calcula ROE
        df_roe['ROE'] = df_roe['LucroLiquido'] / df_roe['PatrimonioLiquido']
        indicadores = indicadores.merge(
            df_roe[['data', 'data_envio', 'ticker', 'ROE']],
            on=['data', 'data_envio', 'ticker'],
            how='left'
        )

    # Salvar todos os indicadores, exceto 'data', 'data_envio', 'ticker'
    for coluna in indicadores.columns:
        if coluna not in ['data', 'data_envio', 'ticker']:
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
    calcular_indicadores(input_path, cotacoes_path, output_path)