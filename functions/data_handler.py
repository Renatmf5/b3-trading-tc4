from .load_data import ScrapingResultados, LoadDatasets
from .indicadores import MakeIndicators


class DataHandler:
    def __init__(self):
        # Inicializar os componentes necessários
        self.datasets_loader = None
        self.scraping = None

    def carregar_datasets(self):
        """
        Função para carregar os datasets necessários.
        """
        self.datasets_loader = LoadDatasets()
        print("Carregando datasets...")
        #self.datasets_loader.get_cdi_last_15_years()
        self.datasets_loader.get_ibovespa_last_15_years()
        #self.datasets_loader.atualizar_acoes_consolidado()
        print("Datasets carregados com sucesso.")
        return True

    def chama_scraping(self):
        """
        Configura o scraping com os parâmetros necessários.
        """
        print("Configurando scraping...")
        self.scraping = ScrapingResultados()
        try:
            # Processar os tickers e consolidar os dados
            self.scraping.process_table_in_batches(batch_size=2)

        finally:
            self.scraping.fechar_driver()
            print("Scraping executado com sucesso.")
    
    def cria_indicares_tecnicos(self):
        self.indicators = MakeIndicators()
        
        self.indicators.fazendo_indicadores_tecnicos()
