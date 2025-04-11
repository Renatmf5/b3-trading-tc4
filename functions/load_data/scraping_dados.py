import os
import time
import unicodedata
import pandas as pd
import math
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

class ScrapingResultados:
    def __init__(self, input_path= "dados/acoes/acoes_cotacoes.parquet", base_url = "https://www.fundamentus.com.br/resultados_trimestrais.php?papel=", processed_file="processed_dates.txt"):
        self.base_url = base_url
        self.processed_file = processed_file
        self.output_path = "dados/balancos"
        self.input_path = input_path
        self.processed_dates = self.load_processed_dates()
        
        os.makedirs(self.output_path, exist_ok=True)

        # Configurar o Selenium
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--start-maximized")

        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    def load_processed_dates(self):
        """
        Carrega as datas já processadas de um arquivo.
        """
        if os.path.exists(self.processed_file):
            with open(self.processed_file, "r") as file:
                return set(line.strip() for line in file.readlines())
        return set()

    def save_processed_date(self, date):
        """
        Salva uma data processada no arquivo.
        """
        with open(self.processed_file, "a") as file:
            file.write(date + "\n")
        self.processed_dates.add(date)
        
    def processar_tabela_detalhada(self, data_doc, data_envio, ticker):
        """
        Processa a tabela detalhada na página redirecionada e retorna um DataFrame.
        """
        def is_float(value):
            try:
                float(value)
                return True
            except ValueError:
                return False
        
        try:
            # Verificar se a tabela está dentro de um iframe
            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            tabela = None
            for iframe in iframes:
                self.driver.switch_to.frame(iframe)
                try:
                    # Tentar localizar a tabela dentro do iframe
                    tabela = WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.XPATH, '//table[contains(@id, "tbDados")]'))
                    )
                    if tabela:
                        break  # Sair do loop se a tabela for encontrada
                except:
                    self.driver.switch_to.default_content()  # Voltar ao contexto principal se não encontrar
            else:
                # Se a tabela não for encontrada em nenhum iframe, lançar uma exceção
                raise Exception("Tabela não encontrada em nenhum iframe.")

            linhas = tabela.find_elements(By.XPATH, "./tbody/tr")[1:]  # Ignorar o cabeçalho

            # Lista para armazenar os dados
            dados = []

            for linha in linhas:
                colunas = linha.find_elements(By.XPATH, "./td")
                if len(colunas) < 3:
                    continue  # Ignorar linhas incompletas

                # Extrair os valores
                conta = colunas[0].text.strip()
                descricao = colunas[1].text.strip()
                # Normalizar o texto da descrição
                descricao = str(descricao).strip()  # Remove espaços extras
                descricao = unicodedata.normalize('NFKD', descricao).encode('latin1', 'ignore').decode('latin1')  # Corrigir caracteres estranhos
                descricao = ' '.join(descricao.split())  # Remove espaços duplicados
                
                valor_primeiro_periodo = colunas[2].text.strip().replace(".", "").replace(",", ".").replace("\xa0", "")
                
                # Validar se o valor pode ser convertido em float
                valor_convertido = float(valor_primeiro_periodo) if is_float(valor_primeiro_periodo) else 0
                
               
                # Adicionar ao conjunto de dados
                dados.append({
                    "data_doc": data_doc,
                    "data_envio": data_envio,
                    "ticker": ticker,
                    "conta": conta,
                    "descricao": descricao,
                    "valor_primeiro_periodo": valor_convertido
                })

            # Criar um DataFrame
            df = pd.DataFrame(dados)

            # Voltar ao contexto principal após processar a tabela
            self.driver.switch_to.default_content()

            return df

        except Exception as e:
            print(f"Erro ao processar a tabela detalhada: {e}")
            return pd.DataFrame()
    
    def process_table_in_batches(self, batch_size=5):
        """
        Processa os tickers em lotes e salva os resultados localmente.
        """
        df_consolidado = pd.DataFrame()

        self.df_acoes = pd.read_parquet(self.input_path)
        tickers = self.df_acoes['ticker'].sort_values(ascending=True).unique()

        # Dividir os tickers em lotes
        total_tickers = len(tickers)
        total_batches = math.ceil(total_tickers / batch_size)

        # Verificar quais lotes já foram processados
        processed_batches = self.get_processed_batches()

        for batch_index in range(total_batches):
            if batch_index in processed_batches:
                print(f"Lote {batch_index + 1}/{total_batches} já processado. Pulando...")
                continue

            # Obter os tickers do lote atual
            start_index = batch_index * batch_size
            end_index = min(start_index + batch_size, total_tickers)
            batch_tickers = tickers[start_index:end_index]
            print(f"Processando lote {batch_index + 1}/{total_batches} com {len(batch_tickers)} tickers.")

            ticker_count = 0
            # Processar os tickers do lote
            batch_data = pd.DataFrame()
            for ticker in batch_tickers:  # Corrigido para iterar sobre batch_tickers
                ticker_count += 1  # Incrementa o contador
                print(f"Processando ticker: {ticker} [{ticker_count}/{len(batch_tickers)}]")

                # Resetar o arquivo de datas processadas para o ticker atual
                with open(self.processed_file, "w") as file:
                    file.write("")  # Limpar o conteúdo do arquivo
                self.processed_dates = set()

                url = f"{self.base_url}{ticker}"

                # Configurar o tempo limite para carregamento da página
                self.driver.set_page_load_timeout(30)  # 300 segundos (5 minutos)

                self.driver.get(url)
                time.sleep(3)  # Aguarde o carregamento da página

                # Localizar a tabela
                tabela = self.driver.find_element(By.XPATH, '//*[@id="fd-table-1"]')
                linhas = tabela.find_elements(By.XPATH, "./tbody/tr")

                for linha in linhas:
                    # Obter a data da primeira coluna
                    data_referencia = linha.find_element(By.XPATH, './td[1]/span').text.strip()

                    # Verificar se a data já foi processada
                    if data_referencia in self.processed_dates:
                        print(f"Data {data_referencia} já processada. Pulando...")
                        continue

                    # Obter o link "Exibir" da segunda coluna
                    link_exibir = linha.find_element(By.XPATH, './td[2]/a')
                    link_url = link_exibir.get_attribute("href")

                    # Clicar no link "Exibir"
                    self.driver.execute_script("window.open(arguments[0]);", link_url)
                    self.driver.switch_to.window(self.driver.window_handles[-1])

                    # Aguarde o carregamento da página redirecionada
                    time.sleep(5)

                    # Capturar os valores das labels
                    try:
                        data_doc = self.driver.find_element(By.XPATH, '//span[@id="lblDataDocumento"]').text.strip()
                        data_envio = self.driver.find_element(By.XPATH, '//span[@id="lblDataEnvio"]').text.strip()

                        # Extrair apenas a data de data_envio (ignorando o horário)
                        data_envio = data_envio.split(" ")[0]

                        # Opções desejadas
                        opcoes = [
                            "Balanço Patrimonial Ativo",
                            "Demonstração do Resultado",
                            "Balanço Patrimonial Passivo",
                            "Demonstração do Fluxo de Caixa",
                            "Demonstração de Valor Adicionado"
                        ]

                        # DataFrame final para a data atual
                        df_final = pd.DataFrame()

                        # Itera sobre as opções desejadas do select
                        for opcao in opcoes:
                            try:
                                # Localizar o elemento <select> novamente após o postback
                                select_element = WebDriverWait(self.driver, 10).until(
                                    EC.presence_of_element_located((By.XPATH, '//*[@id="cmbQuadro"]'))
                                )
                                select = Select(select_element)

                                # Selecionar a opção desejada
                                select.select_by_visible_text(opcao)

                                # Aguarda o postback e o novo <select> ser recriado
                                WebDriverWait(self.driver, 10).until(
                                    EC.staleness_of(select_element)  # Aguarda o elemento antigo ser removido
                                )
                                select_element = WebDriverWait(self.driver, 10).until(
                                    EC.presence_of_element_located((By.XPATH, '//*[@id="cmbQuadro"]'))
                                )

                                contas_relevantes = [
                                    '1', '1.01', '1.01.01', '1.02', '2', '2.01', '2.01.04', '2.02', '2.02.01', '2.03',
                                    '3.01', '3.02', '3.03', '3.04', '3.04.06', '3.06', '3.06.01', '3.06.02', '3.08',
                                    '3.09', '3.10', '3.11', '3.11.01', '6.03.05', '6.03.06', '7.04.01'
                                ]
                                # Processar a tabela detalhada
                                df_tabela = self.processar_tabela_detalhada(data_doc, data_envio, ticker)
                                # filtrar df_tabela com contas_relevantes
                                df_tabela = df_tabela[df_tabela['conta'].isin(contas_relevantes)]

                                # Concatenar com o DataFrame final
                                df_final = pd.concat([df_final, df_tabela], ignore_index=True)

                            except Exception as e:
                                print(f"Erro ao capturar dados na página redirecionada: {e}")

                        # Consolidar os dados do ticker atual no DataFrame consolidado
                        batch_data = pd.concat([batch_data, df_final], ignore_index=True)

                    except Exception as e:
                        print(f"Erro ao capturar dados na página redirecionada: {e}")

                    # Fechar a aba e voltar para a aba principal
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])

                    # Salvar a data como processada
                    self.save_processed_date(data_referencia)

            # Salvar o lote processado localmente
            batch_file = os.path.join(self.output_path, f"lote_{batch_index}.parquet")
            batch_data.to_parquet(batch_file, index=False)
            print(f"Lote {batch_index + 1}/{total_batches} salvo em {batch_file}.")
        
         # Após processar todos os lotes, concatenar os arquivos e criar o arquivo final
        if len(self.get_processed_batches()) == total_batches:
            print("Todos os lotes foram processados. Concatenando os arquivos...")
            self.concat_batches()

    def get_processed_batches(self):
        """
        Retorna os índices dos lotes já processados com base nos arquivos salvos.
        """
        processed_batches = []
        for file in os.listdir(self.output_path):
            if file.startswith("lote_") and file.endswith(".parquet"):
                batch_index = int(file.split("_")[1].split(".")[0])
                processed_batches.append(batch_index)
        return set(processed_batches)

    def concat_batches(self):
        """
        Concatena todos os lotes processados em um único arquivo e apaga os arquivos de lotes.
        """
        batch_files = [
            os.path.join(self.output_path, file)
            for file in os.listdir(self.output_path)
            if file.startswith("lote_") and file.endswith(".parquet")
        ]

        if not batch_files:
            print("Nenhum lote encontrado para concatenação.")
            return

        # Concatenar todos os lotes
        df_consolidado = pd.concat([pd.read_parquet(batch_file) for batch_file in batch_files], ignore_index=True)

        # Salvar o arquivo consolidado
        consolidated_file = os.path.join(self.output_path, "balancos_consolidados.parquet")
        df_consolidado.to_parquet(consolidated_file, index=False)
        print(f"Arquivo consolidado salvo em {consolidated_file}.")

        # Apagar os arquivos de lotes
        for batch_file in batch_files:
            os.remove(batch_file)
        print("Arquivos de lotes apagados com sucesso.")

    def fechar_driver(self):
        """
        Fecha o driver do Selenium.
        """
        self.driver.quit()
