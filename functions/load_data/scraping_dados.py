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
        
    def capturar_dados_empresa(self, data_doc, data_envio, ticker, df_tabela):
        """
        Captura os dados de "Dados da Empresa" e concatena com o DataFrame fornecido.
        """
        try:
            # Selecionar a opção "Dados da Empresa" no select
            select_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="cmbGrupo"]'))
            )
            select = Select(select_element)
            select.select_by_visible_text("Dados da Empresa")
            time.sleep(3)  # Aguarde o carregamento da tabela

             # Verificar se a tabela está dentro de um iframe
            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            tabela = None
            for iframe in iframes:
                self.driver.switch_to.frame(iframe)
                try:
                    # Tentar localizar a tabela dentro do iframe
                    tabela = WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.XPATH, '//table[contains(@style, "border-left: 1px solid")]'))
                    )
                    if tabela:
                        break  # Sair do loop se a tabela for encontrada
                except:
                    self.driver.switch_to.default_content()  # Voltar ao contexto principal se não encontrar
            else:
                # Se a tabela não for encontrada em nenhum iframe, lançar uma exceção
                raise Exception("Tabela não encontrada em nenhum iframe.")

            # Extrair os dados da tabela
            dados = {
                "ordinarias": tabela.find_element(By.ID, "QtdAordCapiItgz_1").text.strip().replace(".", "").replace(",", "."),
                "preferenciais": tabela.find_element(By.ID, "QtdAprfCapiItgz_1").text.strip().replace(".", "").replace(",", "."),
                "total": tabela.find_element(By.ID, "QtdTotAcaoCapiItgz_1").text.strip().replace(".", "").replace(",", ".")
            }

            # Criar um DataFrame com os dados capturados
            df_empresa = pd.DataFrame([{
                "data_doc": data_doc,
                "data_envio": data_envio,
                "ticker": ticker,
                "qtd_acoes_on": float(dados["ordinarias"]),
                "qtd_acoes_pn": float(dados["preferenciais"]),
                "qtd_acoes_total": float(dados["total"])
            }])

            # Concatenar com o DataFrame fornecido
            df_tabela = pd.merge(df_tabela, df_empresa, on=["data_doc", "data_envio", "ticker"], how="left")

            return df_tabela

        except Exception as e:
            print(f"Erro ao capturar dados de 'Dados da Empresa': {e}")
            return df_tabela
        
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

                        # Inicializar as variáveis antes do loop
                        qtd_acoes_on = None
                        qtd_acoes_pn = None
                        qtd_acoes_total = None
                        # DataFrame final para a data atual
                        df_final = pd.DataFrame()

                        # Itera sobre as opções desejadas do select
                        for i, opcao in enumerate(opcoes):
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
                                    '3.09', '3.10', '3.11', '3.11.01', '7.08.04.01', '7.08.04.02', '7.04.01'
                                ]
                                # Processar a tabela detalhada
                                df_tabela = self.processar_tabela_detalhada(data_doc, data_envio, ticker)
                                # filtrar df_tabela com contas_relevantes
                                df_tabela = df_tabela[df_tabela['conta'].isin(contas_relevantes)]

                                # Capturar os valores de ações na última iteração
                                if i == len(opcoes) - 1:
                                    if not df_tabela.empty:
                                        df_tabela = self.capturar_dados_empresa(data_doc, data_envio, ticker, df_tabela)
                                        # Preencher valores ausentes com o último valor válido
                                        df_tabela[['qtd_acoes_on', 'qtd_acoes_pn', 'qtd_acoes_total']] = df_tabela[['qtd_acoes_on', 'qtd_acoes_pn', 'qtd_acoes_total']].ffill()

                                        # Capturar os últimos valores preenchidos
                                        qtd_acoes_on = df_tabela['qtd_acoes_on'].iloc[-1] if not df_tabela['qtd_acoes_on'].isna().all() else None
                                        qtd_acoes_pn = df_tabela['qtd_acoes_pn'].iloc[-1] if not df_tabela['qtd_acoes_pn'].isna().all() else None
                                        qtd_acoes_total = df_tabela['qtd_acoes_total'].iloc[-1] if not df_tabela['qtd_acoes_total'].isna().all() else None

                                # Concatenar com o DataFrame final
                                df_final = pd.concat([df_final, df_tabela], ignore_index=True)
                                # Atualizar as linhas do df_final para o respectivo ticker e data_doc, se as variáveis de quantidade não forem None
                                if qtd_acoes_on is not None and qtd_acoes_pn is not None and qtd_acoes_total is not None:
                                    df_final.loc[
                                        (df_final['ticker'] == ticker) & (df_final['data_doc'] == data_doc),
                                        ['qtd_acoes_on', 'qtd_acoes_pn', 'qtd_acoes_total']
                                    ] = [qtd_acoes_on, qtd_acoes_pn, qtd_acoes_total]
                                

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
            os.path.join("dados/balancos", file)
            for file in os.listdir("dados/balancos")
            if file.startswith("lote_") and file.endswith(".parquet")
        ]

        if not batch_files:
            print("Nenhum lote encontrado para concatenação.")

        # Concatenar todos os lotes
        df_consolidado = pd.concat([pd.read_parquet(batch_file) for batch_file in batch_files], ignore_index=True)
        df_consolidado.loc[df_consolidado['valor_primeiro_periodo'] == 0, 'valor_primeiro_periodo'] = None

        # Multiplicar o valor de valor_primeiro_periodo para milhares * 1000

        df_consolidado['valor_primeiro_periodo'] = df_consolidado['valor_primeiro_periodo'].apply(lambda x: x * 1000 if isinstance(x, (int, float)) else x)
        # Multiplicar o valor de qtd_acoes_on qtd_acoes_pn e qtd_acoes_total para milhares * 1000
        df_consolidado['qtd_acoes_on'] = df_consolidado['qtd_acoes_on'].apply(lambda x: x * 1000 if isinstance(x, (int, float)) else x)
        df_consolidado['qtd_acoes_pn'] = df_consolidado['qtd_acoes_pn'].apply(lambda x: x * 1000 if isinstance(x, (int, float)) else x)
        df_consolidado['qtd_acoes_total'] = df_consolidado['qtd_acoes_total'].apply(lambda x: x * 1000 if isinstance(x, (int, float)) else x)



        # Garantir que a coluna data_doc esteja no formato datetime
        if 'data_doc' in df_consolidado.columns:
            df_consolidado['data_doc'] = pd.to_datetime(df_consolidado['data_doc'], format='%d/%m/%Y', errors='coerce')

        # Ordenar o DataFrame por ticker, conta e data_doc de forma decrescente
        df_consolidado = df_consolidado.sort_values(by=["ticker", "conta", "data_doc"], ascending=[True, True, True])


        # Preencher valores ausentes no valor_primeiro_periodo com o último valor disponível dentro de cada grupo (ticker e conta)
        if 'valor_primeiro_periodo' in df_consolidado.columns:
            df_consolidado['valor_primeiro_periodo'] = (
                df_consolidado.groupby(['ticker', 'conta'], group_keys=False)['valor_primeiro_periodo']
                .apply(lambda group: group.fillna(method='ffill'))  # Preenche apenas dentro do grupo
            )

        # Filtrar apneas os periodos do ultimos trimestre de cada ano
        df_ultimo_trimeste = df_consolidado[df_consolidado['data_doc'].dt.month == 12]

        # Ajustar quarto trimestre
        # Adicionar uma coluna para o ano
        df_consolidado['ano'] = df_consolidado['data_doc'].dt.year

        # Adicionar uma coluna para o trimestre
        df_consolidado['trimestre'] = ((df_consolidado['data_doc'].dt.month - 1) // 3) + 1

        # Lista de contas que devem ser ajustadas
        contas_para_ajustar = [
            '3.01', '3.02', '3.03', '3.04', '3.04.06', '3.06', '3.06.01', '3.06.02', '3.08',
            '3.09', '3.10', '3.11', '3.11.01'
        ]
        def ajustar_trimestres_70401(grupo):
            """
            Ajusta os valores da conta 7.04.01 para calcular apenas os valores relativos a cada trimestre.
            """
            if grupo['conta'].iloc[0] != '7.04.01':
                return grupo  # Retornar o grupo sem alterações se não for a conta 7.04.01

            # Ordenar o grupo por ano e trimestre
            grupo = grupo.sort_values(by=['ano', 'trimestre'])

            # Calcular os valores relativos a cada trimestre
            valores_corrigidos = []
            acumulado_anterior = 0

            for _, row in grupo.iterrows():
                valor_atual = row['valor_primeiro_periodo']
                if row['trimestre'] == 1:
                    # Para o primeiro trimestre, o valor permanece o mesmo
                    valor_corrigido = valor_atual
                else:
                    # Para os demais trimestres, subtrair o acumulado anterior
                    valor_corrigido = valor_atual - acumulado_anterior

                valores_corrigidos.append(valor_corrigido)
                acumulado_anterior = valor_atual

            # Atualizar os valores no grupo
            grupo['valor_primeiro_periodo'] = valores_corrigidos
            return grupo

        def ajustar_quarto_trimestre(grupo):
            # Verificar se a conta do grupo está na lista de contas para ajustar
            if grupo['conta'].iloc[0] not in contas_para_ajustar:
                return grupo  # Retornar o grupo sem alterações

            # Identificar o último trimestre (trimestre 4)
            ultimo_trimestre = grupo[grupo['trimestre'] == 4]
            if not ultimo_trimestre.empty:
                # Iterar sobre cada ano presente no grupo
                for ano in ultimo_trimestre['ano'].unique():
                    # Filtrar os dados do respectivo ano
                    grupo_ano = grupo[grupo['ano'] == ano]
                    # Calcular a soma dos valores dos três primeiros trimestres do ano
                    soma_primeiros_trimestres = grupo_ano[grupo_ano['trimestre'] < 4]['valor_primeiro_periodo'].sum()
                    # Subtrair a soma dos três primeiros trimestres do valor acumulado do último trimestre
                    grupo.loc[(grupo['trimestre'] == 4) & (grupo['ano'] == ano), 'valor_primeiro_periodo'] -= soma_primeiros_trimestres
            return grupo

        # Aplicar a lógica para cada combinação de ticker e conta
        df_consolidado = df_consolidado.groupby(['ticker', 'conta'], group_keys=False).apply(ajustar_quarto_trimestre)

        # Aplicar abs para contas especificas
        contas_abs = ['3.02', '3.04', '3.06.02', '3.08','7.08.04.01', ' 7.08.04.02', '7.04.01']
        for conta in contas_abs:
            df_consolidado.loc[df_consolidado['conta'] == conta, 'valor_primeiro_periodo'] = df_consolidado.loc[df_consolidado['conta'] == conta, 'valor_primeiro_periodo'].abs()

        # Aplicar a lógica apenas para a conta 7.04.01
        df_consolidado = df_consolidado.groupby(['ticker', 'conta'], group_keys=False).apply(ajustar_trimestres_70401)

        # Preencher valores ausentes com 0
        df_consolidado['valor_primeiro_periodo'].fillna(0, inplace=True)

        # Salvar o arquivo consolidado
        consolidated_file = os.path.join("dados/balancos", "balancos_consolidados.parquet")
        df_ultimo_trimeste = os.path.join("dados/balancos", "balancos_consolidados12m.parquet")
        df_consolidado.to_parquet(consolidated_file, index=False)
        print(f"Arquivo consolidado salvo em {consolidated_file}.")


    def fechar_driver(self):
        """
        Fecha o driver do Selenium.
        """
        self.driver.quit()
