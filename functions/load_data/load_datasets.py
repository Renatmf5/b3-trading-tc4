# load_datasets.py
import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import yfinance as yf
import math

class LoadDatasets:
    def __init__(self, base_dir="dados"):
        """
        Inicializa a classe com o diretório base para salvar os dados.
        """
        self.base_dir = base_dir
        self.indicadores_dir = os.path.join(base_dir, "indicadores")
        self.acoes_dir = os.path.join(base_dir, "acoes")
        os.makedirs(self.indicadores_dir, exist_ok=True)
        os.makedirs(self.acoes_dir, exist_ok=True)

    def get_cdi_history(self, start_date, end_date):
        """
        Consulta o histórico do CDI entre as datas especificadas.
        """
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial={start_date}&dataFinal={end_date}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
            df["valor"] = pd.to_numeric(df["valor"]) / 100
            df = df.rename(columns={"valor": "retorno"})
            return df
        else:
            print(f"Erro ao acessar a API do Banco Central: {response.status_code}")
            return None

    def get_cdi_last_15_years(self):
        """
        Consulta o histórico do CDI dos últimos 15 anos.
        """
        today = datetime.today()
        ten_years_ago = today - timedelta(days=365 * 10)
        fifteen_years_ago = today - timedelta(days=365 * 15)

        today_str = today.strftime("%d/%m/%Y")
        ten_years_ago_str = ten_years_ago.strftime("%d/%m/%Y")
        fifteen_years_ago_str = fifteen_years_ago.strftime("%d/%m/%Y")

        print(f"Consultando CDI de {ten_years_ago_str} a {today_str}...")
        df_recent = self.get_cdi_history(ten_years_ago_str, today_str)

        print(f"Consultando CDI de {fifteen_years_ago_str} a {ten_years_ago_str}...")
        df_past = self.get_cdi_history(fifteen_years_ago_str, ten_years_ago_str)

        if df_recent is not None and df_past is not None:
            df_cdi = pd.concat([df_past, df_recent], ignore_index=True)
            df_cdi = df_cdi.sort_values(by="data")
            output_path = os.path.join(self.indicadores_dir, "cdi.parquet")
            df_cdi.to_parquet(output_path, index=False)
            print(f"Dados do CDI salvos em {output_path}")
        else:
            print("Erro ao obter os dados do CDI.")

    def get_ibovespa_last_15_years(self):
        """
        Consulta o histórico do Ibovespa dos últimos 15 anos.
        """
        ticker = "^BVSP"
        end_date = datetime.now()
        start_date = end_date - timedelta(days=15 * 365)

        try:
            df_ibovespa = yf.download(ticker, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
            df_ibovespa.reset_index(inplace=True)
            df_ibovespa['data'] = pd.to_datetime(df_ibovespa["Date"], format="%d/%m/%Y")
            df_ibovespa['indice'] = 'IBOV'
            df_ibovespa = df_ibovespa[['indice', 'data', 'Close']]
            df_ibovespa = df_ibovespa.rename(columns={"Close": "fechamento"})
            df_ibovespa['fechamento'] = df_ibovespa['fechamento'].round(0).astype(int)

            if not df_ibovespa.empty:
                output_path = os.path.join(self.indicadores_dir, "IBOV.parquet")
                df_ibovespa.to_parquet(output_path, index=False)
                print(f"Dados do Ibovespa salvos em {output_path}")
            else:
                print("Nenhum dado encontrado para o índice Ibovespa.")
        except Exception as e:
            print(f"Erro ao obter os dados do Ibovespa: {e}")

    def atualizar_acoes_consolidado(self):
        """
        Atualiza as cotações históricas dos tickers presentes no arquivo consolidado.
        """
        input_path = os.path.join(self.acoes_dir, "acoes_consolidado.parquet")
        output_path = os.path.join(self.acoes_dir, "acoes_cotacoes.parquet")
        temp_dir = os.path.join(self.acoes_dir, "temp")
        batch_size = 10

        os.makedirs(temp_dir, exist_ok=True)
        df_acoes = pd.read_parquet(input_path)
        data_mais_recente = df_acoes['data_pregao'].max()
        df_acoes = df_acoes[df_acoes['data_pregao'] == data_mais_recente]
        tickers = df_acoes['codigo_acao'].unique()
        num_batches = math.ceil(len(tickers) / batch_size)

        print(f"Processando {len(tickers)} tickers em {num_batches} lotes de {batch_size}...")

        for i in range(num_batches):
            batch_tickers = tickers[i * batch_size:(i + 1) * batch_size]
            print(f"Lote {i + 1}/{num_batches} → {batch_tickers}")

            df_historico = pd.DataFrame()

            for ticker in batch_tickers:
                print(f"Buscando dados para: {ticker}")
                try:
                    df_ticker = yf.download(f'{ticker}.SA', start="2014-01-01", auto_adjust=False, progress=False)
                    if df_ticker.empty:
                        print(f"Sem dados para {ticker}")
                        continue

                    df_ticker = df_ticker.reset_index()
                    if isinstance(df_ticker.columns, pd.MultiIndex):
                        df_ticker.columns = df_ticker.columns.get_level_values(0)

                    df_ticker = df_ticker.rename(columns={
                        'Date': 'data',
                        'Adj Close': 'preco_fechamento_ajustado',
                        'Close': 'close',
                        'High': 'high',
                        'Low': 'low',
                        'Open': 'open',
                        'Volume': 'volume'
                    })

                    df_ticker['ticker'] = ticker
                    df_ticker = df_ticker[['data', 'ticker', 'preco_fechamento_ajustado',
                                           'close', 'high', 'low', 'open', 'volume']]
                    df_historico = pd.concat([df_historico, df_ticker], ignore_index=True)
                except Exception as e:
                    print(f"Erro ao buscar {ticker}: {e}")

            temp_file = os.path.join(temp_dir, f"lote_{i + 1}.parquet")
            df_historico.to_parquet(temp_file, index=False)
            print(f"Lote {i + 1} salvo em: {temp_file}")

        print("Concatenando todos os lotes...")
        all_files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith(".parquet")]
        df_final = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)
        df_final = df_final.sort_values('data', ascending=True)
        df_final['fator_ajuste'] = df_final['preco_fechamento_ajustado'] / df_final['close']
        df_final['close'] = df_final['close'] * df_final['fator_ajuste']
        df_final['open'] = df_final['open'] * df_final['fator_ajuste']
        df_final['high'] = df_final['high'] * df_final['fator_ajuste']
        df_final['low'] = df_final['low'] * df_final['fator_ajuste']

        df_final.to_parquet(output_path, index=False)
        print(f"Arquivo final salvo em: {output_path}")

        for f in all_files:
            os.remove(f)
        os.rmdir(temp_dir)
        print("Processo finalizado com sucesso. Arquivos temporários removidos.")