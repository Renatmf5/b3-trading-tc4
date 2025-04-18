import pandas as pd
import yfinance as yf
import os

# Definir posições dos campos conforme especificado
COLS = {
    "data_pregao": (2, 10),
    "tipo_mercado": (24, 27),
    "codigo_acao": (12, 24),
    'nome_empresa': (27, 39),
    "especificacao": (39, 41),
    "open": (56, 69),
    "high": (69, 82),
    "low": (82, 95),
    "close": (108, 121),
    "median": (95, 108),
    "strike": (188, 201),
    "vencimento": (202, 210),
    "negocios": (147, 152),
    "volume": (171, 186),
}

def parse_cotahist(filename):
    
    with open(filename, "r", encoding="latin-1") as file:
        lines = file.readlines()

    acoes_pn = []
    opcoes = []
    
    for line in lines:
        tipo_mercado = line[COLS["tipo_mercado"][0]:COLS["tipo_mercado"][1]]
        codigo_acao = line[COLS["codigo_acao"][0]:COLS["codigo_acao"][1]].strip()
        data_pregao = line[COLS["data_pregao"][0]:COLS["data_pregao"][1]]
        especificacao = line[COLS["especificacao"][0]:COLS["especificacao"][1]].strip()
        nome_empresa = line[COLS["nome_empresa"][0]:COLS["nome_empresa"][1]].strip()

        # Filtrar AÇÕES PN (tipo_mercado == '010' e nome termina com 'PN')
        if tipo_mercado == "010" and (especificacao == "PN" or especificacao == "ON"):
            acoes_pn.append({
                "data_pregao": data_pregao,
                "codigo_acao": codigo_acao,
                "especificacao": especificacao,
                'nome_empresa': nome_empresa,
                "open": int(line[COLS["open"][0]:COLS["open"][1]]) / 100,
                "high": int(line[COLS["high"][0]:COLS["high"][1]]) / 100,
                "low": int(line[COLS["low"][0]:COLS["low"][1]]) / 100,
                "close": int(line[COLS["close"][0]:COLS["close"][1]]) / 100,
                "median": int(line[COLS["median"][0]:COLS["median"][1]]) / 100,
                "negocios": int(line[COLS["negocios"][0]:COLS["negocios"][1]]),
                "volume": int(line[COLS["volume"][0]:COLS["volume"][1]]),
            })
        
        # Filtrar OPÇÕES (tipo_mercado == '070' ou '080')
        elif tipo_mercado in ["070", "080"]:
            opcoes.append({
                "data_pregao": data_pregao,
                "codigo_opcao": codigo_acao,
                "ticket": codigo_acao[:4],  # Assumindo que a relação com a ação está nos primeiros 4 caracteres
                "especificacao": especificacao,
                "open": int(line[COLS["open"][0]:COLS["open"][1]]) / 100,
                "high": int(line[COLS["high"][0]:COLS["high"][1]]) / 100,
                "low": int(line[COLS["low"][0]:COLS["low"][1]]) / 100,
                "close": int(line[COLS["close"][0]:COLS["close"][1]]) / 100,
                "median": int(line[COLS["median"][0]:COLS["median"][1]]) / 100,
                "negocios": int(line[COLS["negocios"][0]:COLS["negocios"][1]]),
                "volume": int(line[COLS["volume"][0]:COLS["volume"][1]]),
                "strike": int(line[COLS["strike"][0]:COLS["strike"][1]]) / 100,
                "vencimento": line[COLS["vencimento"][0]:COLS["vencimento"][1]],
            })

    # Criar dataframes
    df_acoes_pn = pd.DataFrame(acoes_pn).sort_values(by="data_pregao")
    df_opcoes = pd.DataFrame(opcoes).sort_values(by="data_pregao")
    
   
    return df_acoes_pn, df_opcoes
    
# Exemplo de uso

def process_all_files():
    # Caminhos das pastas
    raw_data_path = "dados/raw"
    acoes_output_path = "dados/acoes"
    opcoes_output_path = "dados/opcoes"

    # Garantir que as pastas de saída existam
    os.makedirs(acoes_output_path, exist_ok=True)
    os.makedirs(opcoes_output_path, exist_ok=True)

    # DataFrames consolidados
    df_acoes_consolidado = pd.DataFrame()
    df_opcoes_consolidado = pd.DataFrame()

    # Processar todos os arquivos na pasta raw
    for filename in sorted(os.listdir(raw_data_path)):
        if filename.startswith("COTAHIST") and filename.endswith(".TXT"):
            filepath = os.path.join(raw_data_path, filename)
            print(f"Processando arquivo: {filename}")
            
            # Processar o arquivo
            df_acoes, df_opcoes = parse_cotahist(filepath)
            
            # Adicionar ao DataFrame consolidado
            df_acoes_consolidado = pd.concat([df_acoes_consolidado, df_acoes], ignore_index=True)
            df_opcoes_consolidado = pd.concat([df_opcoes_consolidado, df_opcoes], ignore_index=True)
            
    # remover opções com volume menor que 10000
    df_opcoes_consolidado = df_opcoes_consolidado[df_opcoes_consolidado["volume"] >= 1000]
    
    # captar MarketCap atual
    
    # Calcular média de volume por código de ação
    media_volume = df_acoes_consolidado.groupby("codigo_acao")["volume"].mean().reset_index()
    media_volume.columns = ["codigo_acao", "media_volume"]
    
    
    # Filtrar ações com média de volume >= 100000
    codigos_validos = media_volume[media_volume["media_volume"] >= 100000]["codigo_acao"]
    df_acoes_final = df_acoes_consolidado[df_acoes_consolidado["codigo_acao"].isin(codigos_validos)]
    
    # Identificar ações removidas
    acoes_removidas = df_acoes_consolidado[~df_acoes_consolidado["codigo_acao"].isin(codigos_validos)][["codigo_acao", "especificacao"]]
    
    # Extrair os 4 primeiros caracteres do código de ação removido
    acoes_removidas["ticket"] = acoes_removidas["codigo_acao"].str[:4]

    # Criar uma chave combinada para facilitar o filtro
    acoes_removidas["chave_removida"] = acoes_removidas["ticket"] + "_" + acoes_removidas["especificacao"]
    df_opcoes_consolidado["chave"] = df_opcoes_consolidado["ticket"] + "_" + df_opcoes_consolidado["especificacao"]

    # Filtrar o df_opcoes removendo as opções correspondentes às ações removidas
    df_opcoes_consolidado = df_opcoes_consolidado[~df_opcoes_consolidado["chave"].isin(acoes_removidas["chave_removida"])]

    # Remover a coluna auxiliar "chave" do df_opcoes
    df_opcoes_consolidado = df_opcoes_consolidado.drop(columns=["chave"])

    # Ordenar os DataFrames consolidados por data_pregao
    df_acoes_final["data_pregao"] = pd.to_datetime(df_acoes_final["data_pregao"], format="%Y%m%d")
    df_opcoes_consolidado["data_pregao"] = pd.to_datetime(df_opcoes_consolidado["data_pregao"], format="%Y%m%d")
    df_acoes_final = df_acoes_final.sort_values(by="data_pregao")
    df_opcoes_consolidado = df_opcoes_consolidado.sort_values(by="data_pregao")

    # Salvar os DataFrames consolidados em arquivos .parquet
    df_acoes_final.to_parquet(os.path.join(acoes_output_path, "acoes_consolidado.parquet"), index=False)
    df_opcoes_consolidado.to_parquet(os.path.join(opcoes_output_path, "opcoes_consolidado.parquet"), index=False)

    print("Processamento concluído!")

# Executar o processamento
process_all_files()