import os
import pandas as pd
import numpy as np
import boto3
import json
from indicadores import *
from datetime import timedelta
from modelos import LSTMClassificationModel, LSTMRegressionModel
from sklearn.metrics import accuracy_score, classification_report, mean_absolute_error, mean_squared_error

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
                
                # Ordenar por ticker e data crescente
                lendo_indicador = lendo_indicador.sort_values(by=['ticker', 'data'])
                
                # Calcular a variação entre trimestres
                lendo_indicador['variacao'] = lendo_indicador.groupby('ticker')['valor'].pct_change()
                # Substituir valores 0.0 por NaN
                lendo_indicador['variacao'] = lendo_indicador['variacao'].replace(0.0, pd.NA)
                lendo_indicador['variacao'] = lendo_indicador.groupby('ticker')['variacao'].fillna(method='ffill')

                # Selecionar as colunas finais
                lendo_indicador = lendo_indicador[['data', 'ticker', 'valor', 'variacao']]
                lendo_indicador.columns = ['data', 'ticker', indicador, f'{indicador}_variacao']
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
            grupo['preco_fechamento_futuro'] = grupo['preco_fechamento_ajustado'].shift(-1)

            # Calcular a coluna 'signal'
            grupo['signal'] = (grupo['preco_fechamento_futuro'] > grupo['preco_fechamento_ajustado']).astype(int)

            # Calcular a coluna 'target' como a variação percentual
            grupo['target'] = ((grupo['preco_fechamento_futuro'] - grupo['preco_fechamento_ajustado']) / grupo['preco_fechamento_ajustado']) * 100

            return grupo
        
        self.df_dados = self.df_dados.sort_values(by=['ticker', 'data'])
        
        # Aplicar a função para cada grupo de 'ticker'
        self.df_dados = self.df_dados.groupby('ticker', group_keys=False).apply(calcula_signal_target)
        
        # Remover a coluna temporária 'preco_fechamento_futuro'
        self.df_dados = self.df_dados.drop(columns=['preco_fechamento_futuro'])

        
        return self.df_dados
    
    def BacktestOtimizado(self):
        
        def execute_model_trainer(train_data, test_data, ticker, start_date_test, is_last_loop=False):
            """
            Treina o modelo e realiza predições com base nos dados de treinamento e teste.
            """
            # Trata LSTM
            self.LSTM_model = LSTMClassificationModel(ticker, self.bucket_name, is_last_loop=is_last_loop)
            self.LSTM_regression_model = LSTMRegressionModel(ticker, self.bucket_name, is_last_loop=is_last_loop)

            scaler_LSTM = self.LSTM_model.train_lstm_classification(train_data)
            scaler_LSTM_regression = self.LSTM_regression_model.train_lstm_regression(train_data)
            
            # Usar os últimos 21 dias do treino como entrada para prever os próximos 21 dias
            X_test = train_data.iloc[-21:]  # Últimos 21 dias do treino
            X_test2 = test_data.iloc[:21]
            y_test_classification = test_data.iloc[:21]['signal']  # Primeiros 21 dias do teste (coluna 'signal')
            
            # Concatenar os dois DataFrames
            test_consolidado = pd.concat([X_test, X_test2], axis=0)

            # Fazer predições com o modelo de classificação
            predictions_classification = self.LSTM_model.predict_lstm_classification(test_consolidado, scaler_LSTM)
            binary_predictions = (predictions_classification > 0.5).astype(int)

            # Calcular métricas de classificação
            classification_report_metrics = classification_report(y_test_classification, binary_predictions, output_dict=True)
            test_accuracy = accuracy_score(y_test_classification, binary_predictions)

            print("Relatório de Classificação para teste LSTM:")
            print(classification_report(y_test_classification, binary_predictions))
            
            # Fazer predições com o modelo de regressão
            predictions_regression = self.LSTM_regression_model.predict_lstm_regression(test_consolidado, scaler_LSTM_regression)
            y_test_regression = test_data.iloc[:21]['target']  # Primeiros 21 dias do teste (coluna 'target')
            
            mae = mean_absolute_error(y_test_regression, predictions_regression)
            rmse = np.sqrt(mean_squared_error(y_test_regression, predictions_regression))

            print("Métricas de Regressão:")
            print(f"MAE: {mae}")
            print(f"RMSE: {rmse}")


            # Criar um DataFrame com as métricas combinadas
            metrics_df = pd.DataFrame({
                'ticker': [ticker],
                'periodo_teste': [start_date_test],
                'test_accuracy_classification': [test_accuracy],
                'mae_regression': [mae],
                'rmse_regression': [rmse]
            })

            return metrics_df
        
        self.df_dados['data'] = pd.to_datetime(self.df_dados['data'])
        self.df_dados = self.df_dados.sort_values(by=['ticker','data'])
        
        # Agrupar o DataFrame por 'ticker'
        tickers = ['AALR3','PETR4']#self.df_dados['ticker'].unique()
        all_results = []

        for ticker in tickers:
            print(f"Executando backtest para o ticker: {ticker}")
            
            # Filtrar os dados para o ticker atual
            df_ticker = self.df_dados[self.df_dados['ticker'] == ticker]

            # Definir o período de treinamento (data mínima até 90 dias antes da data máxima)
            start_date = df_ticker['data'].min()
            end_date_train = df_ticker['data'].max() - timedelta(days=90)

            # Definir o período de teste (últimos 90 dias, divididos em 3 períodos de 30 dias)
            start_date_test = end_date_train
            end_date_test = start_date_test + timedelta(days=30)

            results = []

            # Executar o backtest em no máximo 3 períodos de teste
            for i in range(3):
                is_last_loop = (i == 2)
                
                # Filtrar os dados de treinamento e teste
                train_data = df_ticker[(df_ticker['data'] >= start_date) & (df_ticker['data'] < end_date_train)]
                test_data = df_ticker[(df_ticker['data'] >= start_date_test) & (df_ticker['data'] < end_date_test)]

                if test_data.empty:
                    break

                # Executar o treinamento e a predição
                metrics_df = execute_model_trainer(train_data, test_data, ticker, start_date_test, is_last_loop)

                # Verificar se metrics_df é bidimensional
                if metrics_df.ndim == 2:
                    # Armazenar os resultados
                    results.append(metrics_df)
                else:
                    print(f"metrics_df não é bidimensional: {metrics_df.shape}")
                    # Concatenar mesmo assim
                    all_results.append(metrics_df)

                # Atualizar as datas para o próximo período de teste
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
            return None

        # Converter o DataFrame final em JSON estruturado
        backtests_results = final_results_df.groupby('ticker').apply(
            lambda x: x.to_dict(orient='records')
        ).to_dict()
        
        # Converter objetos Timestamp para strings no JSON
        def convert_timestamps(obj):
            if isinstance(obj, pd.Timestamp):
                return obj.strftime('%Y-%m-%d')  # Formato de data como string
            elif isinstance(obj, dict):
                return {k: convert_timestamps(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_timestamps(i) for i in obj]
            else:
                return obj

        backtests_results = convert_timestamps(backtests_results)

        # Salvar o JSON localmente
        json_path = os.path.join('.', 'dados', 'dataModels', 'backtests_results.json')
        with open(json_path, 'w') as json_file:
            json.dump(backtests_results, json_file, indent=4)

        # Enviar o JSON para o Amazon S3
        s3_client = boto3.client('s3')
        s3_client.upload_file(json_path, self.bucket_name, 'backtests_results.json')
        print(f"JSON enviado para o S3: s3://{self.bucket_name}/backtests_results.json")

        return backtests_results
                    

if __name__ == "__main__":
    
    dicionario_indicadores = {
            'indicadores': {
                'momento_1_meses',
                'momento_6_meses',
                'mm_7_40',
                'RSI_14',
            }
        }
    
    
    handler = DataAnalitcsHandler(**dicionario_indicadores)
    handler.cria_data_frame()
    handler.BacktestOtimizado()   
    
    
    
