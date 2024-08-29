from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Cria uma SparkSession
spark = SparkSession.builder \
    .appName("Processamento de Dados") \
    .getOrCreate()

def leitura_json(path_json):
    #Lê dados de um arquivo JSON e retorna um DataFrame do Spark
    return spark.read.json(path_json)

def leitura_csv(path_csv):
    #Lê dados de um arquivo CSV e retorna um DataFrame do Spark
    return spark.read.csv(path_csv, header=True, inferSchema=True)

def leitura_dados(path, tipo_arquivo):
    #Lê dados com base no tipo de arquivo ('csv' ou 'json') e retorna um DataFrame do Spark
    if tipo_arquivo == 'csv':
        return leitura_csv(path)
    elif tipo_arquivo == 'json':
        return leitura_json(path)
    else:
        raise ValueError(f"Tipo de arquivo desconhecido: {tipo_arquivo}")

def rename_columns(df, key_mapping):
    #Renomeia as colunas do DataFrame de acordo com o mapeamento fornecido
    for old_col, new_col in key_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df

def size_data(df):
    #Retorna o número de registros no DataFrame.
    return df.count()

def join(dadosA, dadosB):
    #Combina dois DataFrames em um só (com base na união de todas as linhas
    return dadosA.union(dadosB)

def transformando_dados_tabela(df, nomes_colunas):
    # Transforma os dados em formato de tabela, com base nos nomes das colunas já fornecidos
    for coluna in nomes_colunas:
        if coluna not in df.columns:
            df = df.withColumn(coluna, lit('Indisponivel'))
    df = df.select(*nomes_colunas)
    return df

def salvando_dados(df, path):
    #Salva o DataFrame em um arquivo CSV.
    df.write.csv(path, header=True, mode='overwrite')

if __name__ == "__main__":
    # Caminhos para os arquivos JSON e CSV
    path_json = 'data_raw/dados_empresaA.json'
    path_csv = 'data_raw/dados_empresaB.csv'
    
    #   Leitura dos dados JSON
    df_json = leitura_dados(path_json, 'json')
    nome_colunas_json = df_json.columns
    tamanho_dados_json = size_data(df_json)
    
    print(f"Nome colunas dados json: {nome_colunas_json}")
    print(f"Tamanho dos dados json: {tamanho_dados_json}")

    # Leitura dos dados CSV
    df_csv = leitura_dados(path_csv, 'csv')
    nome_colunas_csv = df_csv.columns
    tamanho_dados_csv = size_data(df_csv)
    
    print(f"Nome colunas dados csv: {nome_colunas_csv}")
    print(f"Tamanho dos dados csv: {tamanho_dados_csv}")

    # Mapeamento de chaves para renomear as colunas dos dados CSV
    key_mapping = {
        'Nome do Item': 'Nome do Produto',
        'Classificação do Produto': 'Categoria do Produto',
        'Valor em Reais (R$)': 'Preço do Produto (R$)',
        'Quantidade em Estoque': 'Quantidade em Estoque',
        'Nome da Loja': 'Filial',
        'Data da Venda': 'Data da Venda'
    }
    
    # Renomear colunas dos dados CSV
    df_csv = rename_columns(df_csv, key_mapping)
    nome_colunas_csv = df_csv.columns
    
    print(f"Nome colunas dados csv após renomear: {nome_colunas_csv}")

    # Combinar dados JSON e CSV
    df_fusao = join(df_json, df_csv)
    nome_colunas_fusao = df_fusao.columns
    tamanho_dados_fusao = size_data(df_fusao)
    
    print(f"Nome colunas dados combinados: {nome_colunas_fusao}")
    print(f"Tamanho dos dados combinados: {tamanho_dados_fusao}")

    #  Transformar dados combinados em formato de tabela
    df_fusao_tabela = transformando_dados_tabela(df_fusao, nome_colunas_fusao)

    # Caminho para salvar o arquivo CSV com os dados combinados
    path_dados_combinados = 'data_processed/dados_combinados.csv'

    # Salvar os dados combinados em um arquivo CSV
    salvando_dados(df_fusao_tabela, path_dados_combinados)

    print(f"Dados combinados salvos em: {path_dados_combinados}")

    # Finaliza a SparkSession
    spark.stop()

