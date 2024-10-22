### IMPORTS 

import os
import requests
import pandas as pd
import sqlite3

### EXTRAÇÃO DOS DADOS

# URL base para os datasets do IMDb
base_url = "https://datasets.imdbws.com/"

# Lista de nomes de arquivos que você deseja baixar
arquivos = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz"
]

# Diretório de destino
destino_diretorio = "data"

# Certifique-se de que o diretório de destino existe
os.makedirs(destino_diretorio, exist_ok=True)

# Loop para baixar cada arquivo
for arquivo in arquivos:
    url = base_url + arquivo
    caminho_destino = os.path.join(destino_diretorio, arquivo)

    # Verifica se o arquivo já existe para evitar o download repetido
    if not os.path.exists(caminho_destino):
        print(f"Baixando {arquivo}...")
        response = requests.get(url)

        # Verifica se a solicitação foi bem-sucedida (código de status 200)
        if response.status_code == 200:
            with open(caminho_destino, 'wb') as f:
                f.write(response.content)
            print(f"{arquivo} baixado com sucesso!")
        else:
            print(f"Falha ao baixar {arquivo}. Código de status: {response.status_code}")
    else:
        print(f"{arquivo} já existe. Pulando o download.")

print("Download concluído.")

### TRANSFORMAÇÃO DOS DADOS

# Diretórios
diretorio_dados = "data"
diretorio_tratados = os.path.join(diretorio_dados, "tratados")

# Certifica-se de que o diretório "tratados" existe
os.makedirs(diretorio_tratados, exist_ok=True)

# Lista todos os arquivos no diretório "data"
arquivos = os.listdir(diretorio_dados)

# Loop para abrir, tratar e salvar cada arquivo
for arquivo in arquivos:
    caminho_arquivo = os.path.join(diretorio_dados, arquivo)

    if os.path.isfile(caminho_arquivo) and arquivo.endswith(".gz"):
        print(f"Lendo e tratando o arquivo {arquivo}...")
        
        # Lê o arquivo TSV usando o pandas
        df = pd.read_csv(caminho_arquivo, sep='\t', compression='gzip', low_memory=False)

        # Substitui os caracteres "\n" por um valor nulo
        df.replace({"\\N": None}, inplace=True)

        # Salva o DataFrame no diretório "tratados" sem compressão
        caminho_destino = os.path.join(diretorio_tratados, arquivo[:-3])  # Remove a extensão .gz
        df.to_csv(caminho_destino, sep='\t', index=False)

        print(f"Tratamento concluído para {arquivo}. Arquivo tratado salvo em {caminho_destino}")

print("Todos os arquivos foram tratados e salvos no diretório 'tratados'.")

### CARGA DOS DADOS

# Diretórios
diretorio_tratados = os.path.join("data", "tratados")
banco_dados = "imdb_data.db"

# Conecta ao banco de dados SQLite
conexao = sqlite3.connect(banco_dados)

# Lista todos os arquivos no diretório "tratados"
arquivos = os.listdir(diretorio_tratados)

# Loop para ler cada arquivo e salvar em uma tabela SQLite
for arquivo in arquivos:
    caminho_arquivo = os.path.join(diretorio_tratados, arquivo)

    if os.path.isfile(caminho_arquivo) and arquivo.endswith(".tsv"):
        # Lê o arquivo TSV usando o pandas
        df = pd.read_csv(caminho_arquivo, sep='\t', low_memory=False)

        # Remove a extensão do nome do arquivo
        nome_tabela = os.path.splitext(arquivo)[0]

        # Substitui os caracteres especiais no nome da tabela
        nome_tabela = nome_tabela.replace(".", "_").replace("-", "_")

        # Salva o DataFrame na tabela SQLite
        df.to_sql(nome_tabela, conexao, index=False, if_exists='replace')

        print(f"Arquivo {arquivo} salvo como tabela {nome_tabela} no banco de dados.")

# Fecha a conexão com o banco de dados
conexao.close()

print("Todos os arquivos foram salvos no banco de dados.")

### CRIAÇÃO DAS TABELAS ANALÍTICAS

analitico_titulos = """
CREATE TABLE IF NOT EXISTS analitico_titulos AS

WITH 
participantes AS (
    SELECT
        tconst,
        COUNT(DISTINCT nconst) as qtParticipantes
    
    FROM title_principals
    
    GROUP BY 1
)

SELECT
    tb.tconst,
    tb.titleType,
    tb.originalTitle,
    tb.startYear,
    tb.endYear,
    tb.genres,
    tr.averageRating,
    tr.numVotes,
    tp.qtParticipantes

FROM title_basics tb 

LEFT JOIN title_ratings tr
    ON tr.tconst = tb.tconst

LEFT JOIN participantes tp
    ON tp.tconst = tb.tconst
"""

analitico_participantes = """
CREATE TABLE IF NOT EXISTS analitico_participantes AS

SELECT
    tp.nconst,
    tp.tconst,
    tp.ordering,
    tp.category,
    tb.genres

FROM title_principals tp

LEFT JOIN title_basics tb
    ON tb.tconst = tp.tconst
"""

#lista de consultas
queries = [analitico_titulos, analitico_participantes]

#salva as tabelas analíticas no banco de dados
for query in queries:

    # Diretórios
    banco_dados = "imdb_data.db"
    
    # Conecta ao banco de dados SQLite
    conexao = sqlite3.connect(banco_dados)
    
    # Consulta SQL para contar o número de pessoas participantes por título
    query = query
    
    # Executa a consulta SQL
    conexao.execute(query)
    
    # Fecha a conexão com o banco de dados
    conexao.close()
    
print("Tabelas criadas com sucesso.")