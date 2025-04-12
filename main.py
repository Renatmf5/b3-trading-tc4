from functions.data_handler import DataHandler


if __name__ == "__main__":

    
    # Instancia o DataHandler
    handler = DataHandler()

    # Carregar os datasets
    #handler.carregar_datasets()
    
    """
    """
    # Configurar e executar o scraping
    input_path = "dados/acoes/acoes_cotacoes.parquet"
    output_dir = "dados/indicadores/"
    indicadores = ['ebitda', 'roe', 'divida_bruta']
    handler.chama_scraping()
    