# load_datasets.py
import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import yfinance as yf
import math
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

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
            output_path = os.path.join(self.acoes_dir, "cdi.parquet")
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
            df_ibovespa = df_ibovespa.reset_index()

            # Verifica se as colunas são MultiIndex e ajusta
            if isinstance(df_ibovespa.columns, pd.MultiIndex):
                df_ibovespa.columns = df_ibovespa.columns.get_level_values(0)

            df_ibovespa['data'] = pd.to_datetime(df_ibovespa["Date"], format="%d/%m/%Y")
            df_ibovespa['indice'] = 'IBOV'
            df_ibovespa = df_ibovespa[['indice', 'data', 'Close']]
            df_ibovespa = df_ibovespa.rename(columns={"Close": "fechamento"})
            df_ibovespa['fechamento'] = df_ibovespa['fechamento'].round(0).astype(int)

            if not df_ibovespa.empty:
                output_path = os.path.join(self.acoes_dir, "IBOV.parquet")
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
                    df_ticker = yf.download(f'{ticker}.SA', start="2010-01-01", auto_adjust=False, progress=False)
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
                    })

                    df_ticker['ticker'] = ticker
                    df_ticker = df_ticker[['data', 'ticker', 'preco_fechamento_ajustado',
                                           'close', 'high', 'low', 'open']]
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
        df_final['open_ajustado'] = df_final['open'] * df_final['fator_ajuste']
        df_final['high_ajustado'] = df_final['high'] * df_final['fator_ajuste']
        df_final['low_ajustado'] = df_final['low'] * df_final['fator_ajuste']
        
        df_consolidado = self.encontrar_acoes_nao_presentes()

        df_consolidado = df_consolidado.rename(columns={
            'open': 'open_hist',
            'high': 'high_hist',
            'low': 'low_hist',
            'close': 'close_hist',
            'especificacao': 'tipo'
        })

        # Realizar o join com base em 'ticker' e 'data'
        df_final = df_final.merge(
            df_consolidado[['data_pregao', 'codigo_acao', 'open_hist', 'high_hist', 'low_hist', 'close_hist', 'volume']],
            left_on=['data', 'ticker'],
            right_on=['data_pregao', 'codigo_acao'],
            how='left'
        )

        # Remover colunas desnecessárias após o join
        df_final = df_final.drop(columns=['data_pregao', 'codigo_acao'])

        # Salvar o DataFrame final
        df_final.to_parquet(output_path, index=False)
        print(f"Arquivo final salvo em: {output_path}")

        # Remover arquivos temporários
        for f in all_files:
            os.remove(f)
        os.rmdir(temp_dir)
        print("Processo finalizado com sucesso. Arquivos temporários removidos.")
    
    def encontrar_acoes_nao_presentes(self):
        """
        Encontra os códigos de ações presentes no arquivo 'acoes_consolidado.parquet'
        que não estão no arquivo 'acoes_cotacoes.parquet'.
        """
        consolidado_path = os.path.join(self.base_dir, "acoes", "acoes_consolidado.parquet")
        cotacoes_path = os.path.join(self.base_dir, "acoes", "acoes_cotacoes.parquet")

        # Ler os arquivos Parquet
        df_consolidado = pd.read_parquet(consolidado_path)
        df_cotacoes = pd.read_parquet(cotacoes_path)

        # Obter os códigos únicos
        codigos_consolidado = set(df_consolidado['codigo_acao'].unique())
        tickers_cotacoes = set(df_cotacoes['ticker'].unique())

        # Encontrar os códigos que estão no consolidado mas não nas cotações
        codigos_nao_presentes = codigos_consolidado - tickers_cotacoes
        
        lista_empresas = self.buscar_nome_empresa(codigos_nao_presentes)

        print(f"Códigos de ações não presentes em 'acoes_cotacoes.parquet': {codigos_nao_presentes}")
        return lista_empresas
    
    def buscar_nome_empresa(self, codigos_nao_presentes):
        input_path = os.path.join(self.acoes_dir, "acoes_consolidado.parquet")
        """
        Realiza web scraping para buscar o nome da empresa associado a cada ticker.
        """
        # Configurar o Selenium
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--start-maximized")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

        resultados = []

        for index, ticker in enumerate(codigos_nao_presentes, start=1):
            url = f"https://www.fundamentus.com.br/detalhes.php?papel={ticker}"
            driver.get(url)

            try:
                # Verificar se a tabela com id "test1" existe
                tabela = WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located((By.ID, "test1"))
                )
                if tabela:
                    # Entrar no tbody e pegar o valor da segunda <td>
                    tbody = tabela.find_element(By.TAG_NAME, "tbody")
                    segunda_td = tbody.find_elements(By.TAG_NAME, "td")[1].text
                    resultados.append({"ticker": ticker, "Nome Empresa": segunda_td})
            except:
                # Se a tabela não existir, passar para o próximo ticker
                print(f"Tabela não encontrada para o ticker: {ticker}")
                continue

            # Log do progresso
            print(f"Lidos [{index}/{len(codigos_nao_presentes)}] tickers")

        driver.quit()

        # Criar um DataFrame com os resultados
        df_resultados = pd.DataFrame(resultados)
        
        # Ler o arquivo acoes_consolidado.parquet
        df_acoes_consolidado = pd.read_parquet(input_path)

        # Adicionar a coluna codigo_acao ao df_resultados
        codigos_acao = []

        for _, row in df_resultados.iterrows():
            nome_empresa = row["Nome Empresa"]

            # Filtrar o DataFrame pelo nome da empresa
            filtro_empresa = df_acoes_consolidado[df_acoes_consolidado["nome_empresa"] == nome_empresa]

            if not filtro_empresa.empty:
                # Ordenar pela data mais recente
                filtro_empresa = filtro_empresa.sort_values(by="data_pregao", ascending=False)

                # Pegar o valor de codigo_acao mais recente
                codigo_acao = filtro_empresa.iloc[0]["codigo_acao"]
            else:
                codigo_acao = None  # Caso não encontre a empresa

            codigos_acao.append(codigo_acao)

        # Adicionar a coluna codigo_acao ao DataFrame df_resultados
        df_resultados["codigo_acao"] = codigos_acao
        
        # Iterar sobre o DataFrame df_resultados
        for _, row in df_resultados.iterrows():
            ticker = row["ticker"]
            novo_codigo_acao = row["codigo_acao"]

            # Localizar o registro correspondente no DataFrame df_acoes_consolidado
            filtro = df_acoes_consolidado["codigo_acao"] == ticker

            if not df_acoes_consolidado[filtro].empty:
                # Atualizar o valor de codigo_acao
                df_acoes_consolidado.loc[filtro, "codigo_acao"] = novo_codigo_acao
                print(f"Atualizado: Ticker {ticker}, Código Antigo -> Novo: {df_acoes_consolidado[filtro]['codigo_acao'].values[0]} -> {novo_codigo_acao}")
        
        
        return df_acoes_consolidado
    
