import pandas as pd
import os
import numpy as np
from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm

class MakeIndicators:
    def __init__(self, input_path='dados/acoes', output_path='dados/indicadores'):
        self.input_path = input_path
        self.output_path = output_path
        self.cotacoes = None
        
        # Garantir que o diretório de saída exista
        os.makedirs(self.output_path, exist_ok=True)
        
    def load_data(self):
        """
        Carrega os dados do arquivo Parquet.
        """
        cotacoes_path = os.path.join(self.input_path, 'acoes_cotacoes.parquet')
        ibov_path = os.path.join(self.input_path, 'IBOV.parquet')
        
        self.cotacoes = pd.read_parquet(cotacoes_path)
        self.cotacoes['data'] = pd.to_datetime(self.cotacoes['data']).dt.date
        self.cotacoes = self.cotacoes[['data', 'ticker', 'volume', 'preco_fechamento_ajustado']]
        self.cotacoes['retorno'] = self.cotacoes.groupby('ticker')['preco_fechamento_ajustado'].pct_change()
        self.cotacoes_ibov = pd.read_parquet(ibov_path)
        print(f"Dados carregados de {self.input_path}")

    def save_data(self, df, filename):
        """
        Salva o DataFrame atualizado no diretório de saída.
        """
        output_file = os.path.join(self.output_path, filename)
        df.to_parquet(output_file, index=False)
        print(f"Indicador salvo em {output_file}")
        
    def volume_mediano(self):

        self.cotacoes['volume'] = self.cotacoes.groupby('ticker')['volume'].fillna(0)
        self.cotacoes['valor'] = self.cotacoes.groupby('ticker')['volume'].rolling(21).median().reset_index(0,drop=True)
        self.cotacoes = self.cotacoes.dropna()
        valor = self.cotacoes[['data', 'ticker', 'valor']]

        self.save_data(valor, 'volume_mediano.parquet')
        
    def calcular_beta(self, anos):
        """
        Calcula o Beta para cada empresa em relação ao IBOV.
        """
        self.cotacoes_ibov['retorno_ibov'] = self.cotacoes_ibov['fechamento'].pct_change()
        self.cotacoes_ibov = self.cotacoes_ibov[['data', 'retorno_ibov']]
        self.cotacoes_ibov['data'] = pd.to_datetime(self.cotacoes_ibov['data']).dt.date

        self.cotacoes['data'] = pd.to_datetime(self.cotacoes['data']).dt.date
        self.cotacoes['retorno'] = self.cotacoes.groupby('ticker')['preco_fechamento_ajustado'].pct_change()
        self.cotacoes.loc[self.cotacoes['retorno'] == 0, 'retorno'] = pd.NA
        self.cotacoes.loc[self.cotacoes['retorno'] == np.inf, 'retorno'] = pd.NA

        dados_totais = pd.merge(self.cotacoes, self.cotacoes_ibov, on='data', how='inner')

        empresas = dados_totais['ticker'].unique()
        dados_totais = dados_totais.set_index('ticker')
        lista_df_betas = []

        for empresa in empresas:
            dado_empresa = dados_totais.loc[empresa]

            if not dado_empresa.dropna().empty:
                if len(dado_empresa) > int(252 * anos):
                    datas = dado_empresa.data.values
                    exog = sm.add_constant(dado_empresa.retorno_ibov)
                    model = RollingOLS(
                        endog=dado_empresa.retorno.values,
                        exog=exog,
                        window=int(252 * anos),
                        min_nobs=int(252 * anos * 0.8)
                    )
                    betas = model.fit()
                    betas = betas.params
                    dado_empresa = betas.reset_index()
                    dado_empresa['data'] = datas
                    dado_empresa.columns = ['ticker', 'const', 'valor', 'data']
                    dado_empresa = dado_empresa[['data', 'ticker', 'valor']]
                    dado_empresa = dado_empresa.dropna()
                    lista_df_betas.append(dado_empresa)

        betas = pd.concat(lista_df_betas)
        self.save_data(betas, f'beta_{int(252 * anos)}.parquet')
    
    def media_movel_proporcao(self, mm_curta, mm_longa):

        self.cotacoes['media_curta'] = self.cotacoes.groupby('ticker')['preco_fechamento_ajustado'].rolling(window=mm_curta, min_periods=int(mm_curta * 0.8)).mean().reset_index(0,drop=True)
        self.cotacoes['media_longa'] = self.cotacoes.groupby('ticker')['preco_fechamento_ajustado'].rolling(window=mm_longa, min_periods=int(mm_longa * 0.8)).mean().reset_index(0,drop=True)
        self.cotacoes['valor'] = self.cotacoes['media_curta']/self.cotacoes['media_longa']
        valor = self.cotacoes[['data', 'ticker', 'valor']]
        valor = valor.dropna()
        
        self.save_data(valor, f'mm_{mm_curta}_{mm_longa}.parquet')
        
    def volatilidade(self, anos):

        self.cotacoes.loc[self.cotacoes['retorno'] == 0, 'retorno'] = pd.NA
        self.cotacoes.loc[self.cotacoes['retorno'] == np.inf, 'retorno'] = pd.NA
        self.cotacoes['valor'] = self.cotacoes.groupby('ticker')['retorno'].rolling(window=int(252 * anos), min_periods=int(252 * anos * 0.8)).std().reset_index(0,drop=True)
        self.cotacoes = self.cotacoes.dropna()
        self.cotacoes['valor'] = self.cotacoes['valor'] * np.sqrt(252) 
        valor = self.cotacoes[['data', 'ticker', 'valor']]
        
        self.save_data(valor,f'vol_{int(252 * anos)}.parquet')

    def calcular_rsi(self, periodo=14):
        """
        Calcula o Índice de Força Relativa (RSI).
        """
        delta = self.cotacoes['preco_fechamento_ajustado'].diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(window=periodo).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=periodo).mean()
        rs = gain / loss
        self.cotacoes['valor'] = 100 - (100 / (1 + rs))
        self.save_data(self.cotacoes[['data', 'ticker', 'valor']], f'RSI_{periodo}.parquet')

    def fazer_indicador_momento(self, meses):

        self.cotacoes['valor'] = self.cotacoes.groupby('ticker')['preco_fechamento_ajustado'].pct_change(periods = (meses * 21))
        self.cotacoes.loc[self.cotacoes['valor'] == 0, 'valor'] = pd.NA
        self.cotacoes.loc[self.cotacoes['valor'] == np.inf, 'valor'] = pd.NA
        self.cotacoes = self.cotacoes.dropna()
        valor = self.cotacoes[['data', 'ticker', 'valor']]
        
        self.save_data(valor, f'momento_{meses}_meses.parquet')

    def fazendo_indicadores_tecnicos(self):
        """
        Função principal que chama todas as funções para calcular os indicadores técnicos.
        """
        self.load_data()
        #self.calcular_beta(anos=1)  # Exemplo: Beta de 3 ano
        #self.volatilidade(anos=1)
        self.calcular_rsi()
        #self.volume_mediano()
        #self.media_movel_proporcao(mm_curta=7, mm_longa=40)
        #self.fazer_indicador_momento(meses=12)
        #self.fazer_indicador_momento(meses=1)
        #self.fazer_indicador_momento(meses=6)

if __name__ == "__main__":
    indicadores = MakeIndicators()
    indicadores.fazendo_indicadores_tecnicos()