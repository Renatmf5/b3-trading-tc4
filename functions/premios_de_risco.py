import pandas as pd
import numpy as np
from dateutil.relativedelta  import relativedelta
import os

class premio_risco:

    def __init__(self, indicadores_dict, nome_premio, liquidez=0):

        self.indicadores = list(indicadores_dict.keys())
        self.ordem_indicadores = list(indicadores_dict.values())
        self.liquidez = liquidez
        self.nome_premio = nome_premio

    def pegando_dados_cotacoes(self):

        self.cotacoes = pd.read_parquet(os.path.join('.', 'dados', 'acoes', 'acoes_cotacoes.parquet'))
        self.cotacoes['id_dado'] = self.cotacoes['ticker'].astype(str) + "_" + self.cotacoes['data'].astype(str)
        self.cotacoes['data'] = pd.to_datetime(self.cotacoes['data']).dt.date

    def pegando_datas_possiveis(self):

        cot_petr = self.cotacoes[self.cotacoes['ticker'] == 'PETR4']

        cot_petr = cot_petr.sort_values('data', ascending = True)
        cot_petr = cot_petr.assign(year = pd.DatetimeIndex(cot_petr['data']).year)
        cot_petr = cot_petr.assign(month = pd.DatetimeIndex(cot_petr['data']).month)
        datas_final_mes = cot_petr.groupby(['year', 'month'])['data'].last()
        datas_final_mes = datas_final_mes.reset_index()

        self.datas_final_mes = datas_final_mes

    def filtrando_volume(self):

        dados_volume = pd.read_parquet(os.path.join('.', 'dados', 'indicadores', 'volume_mediano.parquet'))
        dados_volume['id_dado'] = dados_volume['ticker'].astype(str) + "_" + dados_volume['data'].astype(str)
        dados_volume = dados_volume[['id_dado', 'valor']]
        dados_volume.columns = ['id_dado', 'volumeMediano']
        self.cotacoes = pd.merge(self.cotacoes, dados_volume, how = 'inner', on = 'id_dado')
        self.cotacoes = self.cotacoes[self.cotacoes['volumeMediano'] > self.liquidez]


    def pegando_indicadores(self):

        self.df_indicadores = []
        for indicador in self.indicadores:
            try:
                df = pd.read_parquet(os.path.join('.', 'dados', 'indicadores', f'{indicador}.parquet'))
                df['data'] = pd.to_datetime(df['data']).dt.date
            except:
                df = None
            self.df_indicadores.append(df)

    def descobrindo_mes_inicial(self):
        datas = []
        for df in self.df_indicadores:
            indicador_sem_na = df.dropna()
            petr_indicador = indicador_sem_na.query('ticker == "PETR4"') #eu usei a PETR como parametro pra saber a primeira data de qlqr indicador.
            data_minima = min(petr_indicador['data'])
            datas.append(data_minima)
        data_minima_geral = max(datas)  #maximo porque queremos a MAIOR data mínima entre os indicadores.
        self.data_minima_geral = data_minima_geral + relativedelta(months=+2)
         #vou colocar 2 meses depois da primeira empresa ter o indicador. isso pq os indicadores fundamentalistas no 
        #1 mes da base podem ser muito escassos devido a maioria das empresas não terem soltado resultado ainda.
        self.lista_datas_final_mes = (self.datas_final_mes.query('data >= @self.data_minima_geral'))['data'].to_list()
        self.cotacoes_filtrado = self.cotacoes.query('data >= @self.data_minima_geral')

    def calculando_premios(self):

        colunas = ['primeiro_quartil', 'segundo_quartil', 'terceiro_quartil', 'quarto_quartil', 'universo']
        df_premios = pd.DataFrame(columns=colunas, index=self.lista_datas_final_mes)

        lista_dfs = [None] * 4  # inicializa lista para guardar dataframes de quartis

        for i, data in enumerate(self.lista_datas_final_mes):

            df_info_pontuais = self.cotacoes_filtrado[self.cotacoes_filtrado['data'] == data][['ticker', 'preco_fechamento_ajustado', 'volume']]

            #calculando rent da carteira anterior
            if i != 0:
                df_vendas = df_info_pontuais[['ticker', 'preco_fechamento_ajustado']]
                df_vendas.columns = ['ticker', 'preco_fechamento_ajustado_posterior']
                lista_retornos = []

                for zeta, df in enumerate(lista_dfs):
                    df_quartil = pd.merge(df, df_vendas, how='inner', on='ticker')
                    df_quartil['retorno'] = df_quartil['preco_fechamento_ajustado_posterior'] / df_quartil['preco_fechamento_ajustado'] - 1
                    retorno_quartil = df_quartil['retorno'].mean()
                    lista_retornos.append(retorno_quartil)
                    df_premios.loc[data, colunas[zeta]] = retorno_quartil

                df_premios.loc[data, 'universo'] = np.mean(np.array(lista_retornos))

            #pegando as novas carteiras
            df_info_pontuais['ranking_final'] = 0

            for alfa, indicador in enumerate(self.df_indicadores):
           
                indicador_na_data = indicador.loc[indicador['data'] == data, ['ticker', 'valor']].dropna()
                
                indicador_na_data.columns = ['ticker', f'indicador_{self.indicadores[alfa]}']
                df_info_pontuais = pd.merge(df_info_pontuais, indicador_na_data, how='inner', on='ticker')

            #filtrando empresas com varios tickers pro mais líquido
            df_info_pontuais['comeco_ticker'] = df_info_pontuais['ticker'].astype(str).str[0:4]
            df_info_pontuais.sort_values('volume', ascending=False, inplace=True)
            df_info_pontuais.drop_duplicates('comeco_ticker', inplace=True)
            df_info_pontuais.drop('comeco_ticker', axis=1, inplace=True)

            for alfa, ordem in enumerate(self.ordem_indicadores): 
                crescente_condicao = ordem.lower() == 'crescente'
                df_info_pontuais[f'ranking_{alfa}'] = df_info_pontuais[f'indicador_{self.indicadores[alfa]}'].rank(ascending=crescente_condicao)
                df_info_pontuais['ranking_final'] += df_info_pontuais[f'ranking_{alfa}']

            df_info_pontuais.sort_values('ranking_final', ascending=True, inplace=True)

            empresas_por_quartil = len(df_info_pontuais) // 4
            sobra_empresas = len(df_info_pontuais) % 4

            # dividindo o dataframe em quartis
            lista_dfs[0] = df_info_pontuais.iloc[0: empresas_por_quartil]
            lista_dfs[1] = df_info_pontuais.iloc[empresas_por_quartil: (empresas_por_quartil * 2)]
            lista_dfs[2] = df_info_pontuais.iloc[(empresas_por_quartil * 2): (empresas_por_quartil * 3)]
            lista_dfs[3] = df_info_pontuais.iloc[(empresas_por_quartil * 3): ((empresas_por_quartil * 4) + sobra_empresas)]
    
        df_premios['nome_premio'] = self.nome_premio
        df_premios['liquidez'] = self.liquidez
        df_premios.reset_index(names='data', inplace=True)
        df_premios['id_premio'] = df_premios['nome_premio'].astype(str) + "_" + df_premios['liquidez'].astype(str) + "_" + df_premios['data'].astype(str)
        df_premios.dropna(inplace=True)
        self.df_premios = df_premios

    def colocando_premio_na_base(self):

        self.df_premios.to_parquet(os.path.join(".", "dados", "premios_de_risco", f"{self.nome_premio}_{self.liquidez}.parquet"), index = False) 

if __name__ == "__main__":

    indicadores_dict = {'vol_252': 'decrescente',
                        }
                        
    premio = premio_risco(indicadores_dict,  liquidez = 1000000, nome_premio = 'RISCO_VOL_252_AGRESSIVO', 
                        ) #não pode ter \ no nome!!

    premio.pegando_dados_cotacoes()
    premio.pegando_datas_possiveis()
    premio.filtrando_volume()
    premio.pegando_indicadores()
    premio.descobrindo_mes_inicial()
    premio.calculando_premios()
    premio.colocando_premio_na_base()










