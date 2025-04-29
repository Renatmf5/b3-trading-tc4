import os
import pandas as pd
from indicadores import *
from datetime import timedelta
from modelos import LSTMClassificationModel
from sklearn.metrics import accuracy_score, classification_report

class DataAnalitcsHandler:
    def __init__(self, **kargs):
        
        self.indicadores = kargs
        self.bucket_name = 'models-lake'
        
    
        
    def cria_data_frame(self):
        
        cotacoes = pd.read_parquet(os.path.join('.', 'dados', 'acoes', 'acoes_cotacoes.parquet'))
        cotacoes['data'] = pd.to_datetime(cotacoes['data']).dt.date
        cotacoes['ticker'] = cotacoes['ticker'].astype(str)
        self.cotacoes = cotacoes.sort_values('data', ascending=True)
        
        # Carregar cotações do Ibovespa
        cotacoes_ibov = pd.read_parquet(os.path.join('.', 'dados', 'acoes', 'IBOV.parquet'))
        cotacoes_ibov['data'] = pd.to_datetime(cotacoes_ibov['data']).dt.date
        cotacoes_ibov = cotacoes_ibov.sort_values('data', ascending=True)

        # Calcular pct_change e renomear colunas
        cotacoes_ibov['pct_ibov'] = cotacoes_ibov['fechamento'].pct_change()
        cotacoes_ibov = cotacoes_ibov.rename(columns={'fechamento': 'fechamento_ibov'})
        
        
        cotacoes_cdi = pd.read_parquet(os.path.join('.', 'dados', 'acoes', 'cdi.parquet'))
        cotacoes_cdi['data'] = pd.to_datetime(cotacoes_cdi['data']).dt.date
        cotacoes_cdi = cotacoes_cdi.rename(columns={'retorno': 'retorno_cdi'})
        
        # Concatenar os dados
        df_merged = pd.merge(cotacoes, cotacoes_ibov[['data', 'fechamento_ibov', 'pct_ibov']], on='data', how='left')
        df_merged = pd.merge(df_merged, cotacoes_cdi[['data', 'retorno_cdi']], on='data', how='left')
        
        df_merged['retorno_cdi'] = df_merged['retorno_cdi'].fillna(method='ffill')
        
        # Selecionar as colunas finais
        df_merged = df_merged[['data', 'ticker', 'preco_fechamento_ajustado', 'high', 'low', 'open', 'volume', 'pct_ibov', 'fechamento_ibov', 'retorno_cdi']]

        
        lista_dfs = []

        lista_dfs.append(df_merged)
        lista_indicadores_sem_rep = []

        # Ajuste no loop para o formato do dicionário fornecido
        indicadores = self.indicadores.get('indicadores', set())
        for indicador in indicadores:
            if indicador not in lista_indicadores_sem_rep:
                lista_indicadores_sem_rep.append(indicador)
                lendo_indicador = pd.read_parquet(os.path.join('.', 'dados', 'indicadores', f'{indicador}.parquet')) 
                lendo_indicador['data'] = pd.to_datetime(lendo_indicador['data']).dt.date
                lendo_indicador['ticker'] = lendo_indicador['ticker'].astype(str)
                lendo_indicador['valor'] = lendo_indicador['valor'].astype(float)
                lendo_indicador = lendo_indicador[['data', 'ticker', 'valor']]
                lendo_indicador.columns = ['data', 'ticker', indicador]
                lista_dfs.append(lendo_indicador)

        df_dados = lista_dfs[0]

        for df in lista_dfs[1:]:
            df_dados = pd.merge(df_dados, df,  how='inner', left_on=['data', 'ticker'], right_on=['data', 'ticker'])

        self.df_dados = df_dados.dropna()
        
        # Garantir que o DataFrame esteja ordenado por 'data' dentro de cada 'ticker'
        self.df_dados = self.df_dados.sort_values(by=['ticker', 'data'])
        
        # Função para calcular 'signal' e 'target' para cada grupo de 'ticker'
        def calcula_signal_target(grupo):
            # Deslocar o preço de fechamento ajustado em 21 dias (1 mês útil)
            grupo['preco_fechamento_futuro'] = grupo['preco_fechamento_ajustado'].shift(-21)

            # Calcular a coluna 'signal'
            grupo['signal'] = (grupo['preco_fechamento_futuro'] > grupo['preco_fechamento_ajustado']).astype(int)

            # Calcular a coluna 'target' como a variação percentual
            grupo['target'] = ((grupo['preco_fechamento_futuro'] - grupo['preco_fechamento_ajustado']) / grupo['preco_fechamento_ajustado']) * 100

            return grupo
        
        # Aplicar a função para cada grupo de 'ticker'
        self.df_dados = self.df_dados.groupby('ticker').apply(calcula_signal_target)
        
        # Remover a coluna temporária 'preco_fechamento_futuro'
        self.df_dados = self.df_dados.drop(columns=['preco_fechamento_futuro'])

        
        return self.df
    
    def BacktestOtimizado(self):
        
        def execute_model_trainer(train_data, test_data, ticker):
            """
            Treina o modelo e realiza predições com base nos dados de treinamento e teste.
            """
            # Trata LSTM
           
            self.LSTM_model = LSTMClassificationModel(ticker, self.bucket_name)

            scaler_LSTM = self.LSTM_model.train_lstm_classification(train_data)
            
            test_predictions = self.LSTM_model.predict_lstm_classification(test_data, scaler_LSTM)
            
            # Calcular a acurácia para os dados de treino
            train_predictions = self.LSTM_model.predict_lstm_classification(train_data, scaler_LSTM)
            train_accuracy = accuracy_score(train_data['signal'], train_predictions)

            # Calcular a acurácia para os dados de teste
            test_accuracy = accuracy_score(test_data['signal'], (test_predictions > 0.5).astype(int))

            # Criar um DataFrame com as métricas
            metrics_df = pd.DataFrame({
                'ticker': [ticker],
                'train_accuracy': [train_accuracy],
                'test_accuracy': [test_accuracy]
            })
            print("Relatório de Classificação para teste LSTM:")
            print(classification_report(test_data['signal'], (test_predictions > 0.5).astype(int)))
            
            return metrics_df
        
        self.df_dados['data'] = pd.to_datetime(self.df_dados['data'])
        self.df_dados = self.df_dados.sort_values(by=['ticker','data'])
        
        # Agrupar o DataFrame por 'ticker'
        tickers = self.df_dados['ticker'].unique()
        all_results = []

        for ticker in tickers:
            print(f"Executando backtest para o ticker: {ticker}")
            
            # Filtrar os dados para o ticker atual
            df_ticker = self.df_dados[self.df_dados['ticker'] == ticker]

            # Definir o início e o fim do período de treinamento inicial (2 Anos)
            start_date = df_ticker['data'].min()
            end_date_train = start_date + timedelta(days=365 * 2)

            # Definir o início do período de teste (1 mês após o período de treinamento)
            start_date_test = end_date_train
            end_date_test = start_date_test + timedelta(days=30)

            results = []

            # Executar o backtest até o último período possível
            while not df_ticker[(df_ticker['data'] >= start_date_test) & (df_ticker['data'] < end_date_test)].empty:
                # Filtrar os dados de treinamento e teste
                train_data = df_ticker[(df_ticker['data'] >= start_date) & (df_ticker['data'] < end_date_train)]
                test_data = df_ticker[(df_ticker['data'] >= start_date_test) & (df_ticker['data'] < end_date_test)]

                if test_data.empty:
                    break

                # Executar o treinamento e a predição
                metrics_df = execute_model_trainer(train_data, test_data, ticker)

                # Adicionar colunas de período de teste ao DataFrame de métricas
                metrics_df['start_time'] = start_date_test
                metrics_df['end_time'] = end_date_test
                metrics_df['ticker'] = ticker  # Adicionar o ticker atual

                # Verificar se metrics_df é bidimensional
                if metrics_df.ndim == 2:
                    # Armazenar os resultados
                    results.append(metrics_df)
                else:
                    print(f"metrics_df não é bidimensional: {metrics_df.shape}")

                # Atualizar as datas para a próxima iteração
                end_date_train += timedelta(days=30)
                start_date_test += timedelta(days=30)
                end_date_test += timedelta(days=30)

            # Concatenar os resultados para o ticker atual
            if results:
                ticker_results_df = pd.concat(results, ignore_index=True)
                all_results.append(ticker_results_df)
            else:
                print(f"Nenhum resultado para o ticker {ticker}.")

        # Concatenar todos os resultados de todos os tickers
        if all_results:
            final_results_df = pd.concat(all_results, ignore_index=True)
            print(final_results_df)
        else:
            print("Nenhum resultado para concatenar.")

        return final_results_df if all_results else None
            

if __name__ == "__main__":
    
    dicionario_indicadores = {
            'indicadores': {
                'momento_6_meses',
                'momento_1_meses',
                'L_P',
                'P_VPA',
                'ROA',
                'P_Ativos'
                
            }
        }
    
    
    handler = DataAnalitcsHandler(**dicionario_indicadores)
    handler.cria_data_frame()
    handler.BacktestOtimizado()   
    
    
    
