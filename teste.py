import pandas as pd
import os


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
