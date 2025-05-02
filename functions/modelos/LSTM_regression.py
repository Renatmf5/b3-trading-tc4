import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional
from tensorflow.keras.callbacks import EarlyStopping
import joblib
import boto3
import os

class LSTMRegressionModel:
    def __init__(self, ticker, bucket, is_last_loop=False):
        self.is_last_loop = is_last_loop
        self.ticker = ticker
        self.bucket = bucket
        self.subpasta_modelo = f'models/{ticker}/lstm_regression'
        self.output_location = f's3://{bucket}/{self.subpasta_modelo}/output'
        self.scaler_key = f'models/{ticker}/scaler/{ticker}_lstm_scaler.pkl'
        self.model_path = os.path.join("dados", "dataModels", "lstm_regression_model.keras")
        self.scaler_path = os.path.join("dados", "dataModels", "lstm_regression_scaler.pkl")

    def train_lstm_regression(self, train_data):
        # Normalizar os dados
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(train_data.drop(columns=['data','ticker','preco_fechamento_ajustado','high','low','open','fechamento_ibov','signal', 'target']))

        # Salvar o scaler
        joblib.dump(scaler, self.scaler_path)

        # Preparar os dados para o modelo LSTM
        def create_dataset(data, labels, time_step=1):
            X, y = [], []
            for i in range(len(data) - time_step - 1):
                a = data[i:(i + time_step), :]
                X.append(a)
                y.append(labels[i + time_step])
            return np.array(X), np.array(y)

        time_step = 21
        X, y = create_dataset(scaled_data, train_data['target'].values, time_step)

        # Definir a arquitetura do modelo LSTM
        model = Sequential()
        model.add(Bidirectional(LSTM(64, return_sequences=True, input_shape=(time_step, len(scaled_data[0])))))
        model.add(Dropout(0.1))
        model.add(Bidirectional(LSTM(32, return_sequences=False)))
        model.add(Dropout(0.1))
        model.add(Dense(1))  # Sem ativação sigmoid, pois é uma regressão

        # Compilar o modelo
        model.compile(optimizer='adam', loss='mean_squared_error', metrics=['mae'])

        # Definir o callback para early stopping
        early_stopping = EarlyStopping(monitor='loss', patience=10, restore_best_weights=True)

        # Treinar o modelo
        history = model.fit(X, y, validation_split=0.1, epochs=5, batch_size=32, callbacks=[early_stopping])

        # Salvar o modelo treinado
        model.save(self.model_path)
        
        # Fazer upload do modelo treinado e do scaler para o S3
        #if self.is_last_loop:
            # Fazer upload para o S3
        #    s3_client = boto3.client('s3')
        #    s3_client.upload_file(self.model_path, self.bucket, f'{self.subpasta_modelo}/lstm_regression_model.keras')
        #    s3_client.upload_file(self.scaler_path, self.bucket, self.scaler_key)

        
        return scaler

    def load_model_lstm_regression(self):
        # Carregar o modelo treinado
        model = load_model(self.model_path)
        return model
                
    def predict_lstm_regression(self, test_data, scaler):
        # Carregar o modelo treinado
        model = self.load_model_lstm_regression()

        # Normalizar os dados de teste
        scaled_data = scaler.transform(test_data.drop(columns=['data','ticker','preco_fechamento_ajustado','high','low','open','fechamento_ibov','signal', 'target']))

        # Preparar os dados de teste para o modelo LSTM
        def create_dataset(data, time_step=1):
            X = []
            for i in range(len(data) - time_step):
                a = data[i:(i + time_step), :]
                X.append(a)
            return np.array(X)

        time_step = 21
        X_test = create_dataset(scaled_data, time_step)

        # Fazer predições (retorna valores contínuos, pois é uma regressão)
        predictions = model.predict(X_test)

        return predictions.flatten()