import pandas as pd
import os

df = pd.read_parquet("dados/balancos/balancos_consolidados.parquet")
df

batch_files = [
            os.path.join("dados/balancos", file)
            for file in os.listdir("dados/balancos")
            if file.startswith("lote_") and file.endswith(".parquet")
        ]

if not batch_files:
    print("Nenhum lote encontrado para concatenação.")

# Concatenar todos os lotes
df_consolidado = pd.concat([pd.read_parquet(batch_file) for batch_file in batch_files], ignore_index=True)

# Salvar o arquivo consolidado
consolidated_file = os.path.join("dados/balancos", "balancos_consolidados.parquet")
df_consolidado.to_parquet(consolidated_file, index=False)
print(f"Arquivo consolidado salvo em {consolidated_file}.")