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
    df_cotacoes = pd.read_parquet(cotacoes_file)

    # Renomear coluna de preço de fechamento
    df_cotacoes.rename(columns={'Close': 'preco'}, inplace=True)

    # Mesclar os dados de balanços com as cotações
    df = pd.merge(df_balancos, df_cotacoes, on=['data_doc', 'ticker'], how='left')

    # Função para salvar indicadores
    def salvar_indicador(df_indicador, nome_indicador):
        output_file = os.path.join(output_dir, f"{nome_indicador}.parquet")
        df_indicador.to_parquet(output_file, index=False)
        print(f"Indicador salvo: {output_file}")

    # Cálculo dos indicadores
    indicadores = {}

    # EBIT
    indicadores['EBIT'] = df[df['conta'].isin(['3.03', '3.04'])].groupby(
        ['data_doc', 'data_envio', 'ticker'], as_index=False
    ).apply(lambda x: x.loc[x['conta'] == '3.03', 'valor_primeiro_periodo'].sum() -
                      x.loc[x['conta'] == '3.04', 'valor_primeiro_periodo'].sum())

    # EBITDA
    df_ebit = indicadores['EBIT']
    df_depreciacao = df[df['conta'] == '7.04.01'][['data_doc', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    indicadores['EBITDA'] = pd.merge(df_ebit, df_depreciacao, on=['data_doc', 'data_envio', 'ticker'], how='left')
    indicadores['EBITDA']['valor_primeiro_periodo'] = indicadores['EBITDA']['valor_primeiro_periodo_x'] + \
                                                      indicadores['EBITDA']['valor_primeiro_periodo_y']

    # Dívida Líquida / EBIT
    df_divida_liquida = df[df['conta'].isin(['2.01.04', '2.02.01', '1.01.01'])].groupby(
        ['data_doc', 'data_envio', 'ticker'], as_index=False
    ).apply(lambda x: x.loc[x['conta'].isin(['2.01.04', '2.02.01']), 'valor_primeiro_periodo'].sum() -
                      x.loc[x['conta'] == '1.01.01', 'valor_primeiro_periodo'].sum())
    indicadores['DividaLiquida_EBIT'] = pd.merge(df_divida_liquida, df_ebit, on=['data_doc', 'data_envio', 'ticker'], how='left')
    indicadores['DividaLiquida_EBIT']['valor_primeiro_periodo'] = indicadores['DividaLiquida_EBIT']['valor_primeiro_periodo_x'] / \
                                                                  indicadores['DividaLiquida_EBIT']['valor_primeiro_periodo_y']

    # Margem Bruta
    df_resultado_bruto = df[df['conta'] == '3.03'][['data_doc', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    df_receita_liquida = df[df['conta'] == '3.01'][['data_doc', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    indicadores['MargemBruta'] = pd.merge(df_resultado_bruto, df_receita_liquida, on=['data_doc', 'data_envio', 'ticker'], how='left')
    indicadores['MargemBruta']['valor_primeiro_periodo'] = indicadores['MargemBruta']['valor_primeiro_periodo_x'] / \
                                                           indicadores['MargemBruta']['valor_primeiro_periodo_y']

    # Margem Líquida
    df_lucro_liquido = df[df['conta'] == '3.11'][['data_doc', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    indicadores['MargemLiquida'] = pd.merge(df_lucro_liquido, df_receita_liquida, on=['data_doc', 'data_envio', 'ticker'], how='left')
    indicadores['MargemLiquida']['valor_primeiro_periodo'] = indicadores['MargemLiquida']['valor_primeiro_periodo_x'] / \
                                                             indicadores['MargemLiquida']['valor_primeiro_periodo_y']

    # ROE
    df_patrimonio_liquido = df[df['conta'] == '2.03'][['data_doc', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
    indicadores['ROE'] = pd.merge(df_lucro_liquido, df_patrimonio_liquido, on=['data_doc', 'data_envio', 'ticker'], how='left')
    indicadores['ROE']['valor_primeiro_periodo'] = indicadores['ROE']['valor_primeiro_periodo_x'] / \
                                                   indicadores['ROE']['valor_primeiro_periodo_y']

    # Salvar todos os indicadores
    for nome, df_indicador in indicadores.items():
        salvar_indicador(df_indicador, nome)
        

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
                        ['data_doc', 'data_envio', 'ticker'], as_index=False)['valor_primeiro_periodo'].sum()
                    df_caixa = df[df['conta'] == '1.01.01'][['data_doc', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
                    df_item = pd.merge(df_divida_bruta, df_caixa, on=['data_doc', 'data_envio', 'ticker'], how='left')
                    df_item['valor_primeiro_periodo'] = df_item['valor_primeiro_periodo_x'] - df_item['valor_primeiro_periodo_y']
                    df_item = df_item[['data_doc', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
                else:
                    # Filtra os dados com base na conta
                    df_item = df[df['conta'] == conta][['data_doc', 'data_envio', 'ticker', 'valor_primeiro_periodo']]
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
    dados_path = "dados"
    separar_itens_contabeis(input_path, dados_path)