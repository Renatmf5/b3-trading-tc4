import yfinance as yf
import pandas as pd
import os

# Função para criar diretórios, se não existirem
def criar_diretorio(caminho):
    if not os.path.exists(caminho):
        os.makedirs(caminho)

# Diretórios para salvar os dados
diretorio_acoes = "dados/acoes"
diretorio_opcoes = "dados/opcoes"
criar_diretorio(diretorio_acoes)
criar_diretorio(diretorio_opcoes)

# Lista de ações do índice Bovespa (IBOV)
acoes_ibov = [
    "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "ABEV3.SA",  # Exemplos
    # Adicione mais tickers conforme necessário
]

# DataFrames para armazenar os dados
df_acoes = pd.DataFrame()
df_opcoes = pd.DataFrame()

# Loop para obter dados de cada ação
for ticker in acoes_ibov:
    try:
        # Obter cotações históricas
        acao = yf.Ticker(ticker)
        historico = acao.history(period="max")
        historico["Ticker"] = ticker
        df_acoes = pd.concat([df_acoes, historico])

        # Obter dados de opções
        opcoes = acao.options
        for data_expiracao in opcoes:
            opcoes_data = acao.option_chain(data_expiracao)
            calls = opcoes_data.calls
            puts = opcoes_data.puts
            calls["Tipo"] = "Call"
            puts["Tipo"] = "Put"
            calls["Ticker"] = ticker
            puts["Ticker"] = ticker
            df_opcoes = pd.concat([df_opcoes, calls, puts])

    except Exception as e:
        print(f"Erro ao processar {ticker}: {e}")

# Salvar os DataFrames em arquivos CSV
df_acoes.to_csv(os.path.join(diretorio_acoes, "acoes.csv"), index=False)
df_opcoes.to_csv(os.path.join(diretorio_opcoes, "opcoes.csv"), index=False)

print("Dados salvos com sucesso!")