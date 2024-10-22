import os
import requests
import pandas as pd
import sqlite3
import logging
import schedule
import time
import shutil

# Configuração do logging
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_format)

# Adicione um manipulador de arquivo ao logger
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'script_logs.log')
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(log_format))
logging.getLogger().addHandler(file_handler)

def execute_script():
    print('Início do processo de ETL')

    # EXTRAÇÃO DOS DADOS
    base_url = "https://datasets.imdbws.com/"
    arquivos = [
        "name.basics.tsv.gz",
        "title.akas.tsv.gz",
        "title.basics.tsv.gz",
        "title.crew.tsv.gz",
        "title.episode.tsv.gz",
        "title.principals.tsv.gz",
        "title.ratings.tsv.gz"
    ]
    destino_diretorio = "data"

    os.makedirs(destino_diretorio, exist_ok=True)

    for arquivo in arquivos:
        url = base_url + arquivo
        caminho_destino = os.path.join(destino_diretorio, arquivo)

        if not os.path.exists(caminho_destino):
            logging.info(f"Baixando {arquivo}...")
            response = requests.get(url, stream=True)

            if response.status_code == 200:
                with open(caminho_destino, 'wb') as f:
                    shutil.copyfileobj(response.raw, f)
                logging.info(f"{arquivo} baixado com sucesso!")
            else:
                logging.error(f"Falha ao baixar {arquivo}. Código de status: {response.status_code}")

            del response  # Liberar recursos da resposta
        else:
            logging.info(f"{arquivo} já existe. Pulando o download.")

    logging.info("Download concluído.")

    # TRANSFORMAÇÃO DOS DADOS
    diretorio_dados = "data"
    diretorio_tratados = os.path.join(diretorio_dados, "tratados")

    os.makedirs(diretorio_tratados, exist_ok=True)

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(diretorio_dados, arquivo)

        if os.path.isfile(caminho_arquivo) and arquivo.endswith(".gz"):
            logging.debug(f"Lendo e tratando o arquivo {arquivo}...")

            df = pd.read_csv(caminho_arquivo, sep='\t', compression='gzip', low_memory=False)

            df.replace({"\\N": None}, inplace=True)

            caminho_destino = os.path.join(diretorio_tratados, arquivo[:-3])
            df.to_csv(caminho_destino, sep='\t', index=False)

            logging.debug(f"Tratamento concluído para {arquivo}. Arquivo tratado salvo em {caminho_destino}")

            # Remova o arquivo baixado após o tratamento
            os.remove(caminho_arquivo)

    logging.info("Todos os arquivos foram tratados e salvos no diretório 'tratados'.")

    # CARGA DOS DADOS
    diretorio_tratados = os.path.join("data", "tratados")
    banco_dados = "imdb_data.db"

    conexao = sqlite3.connect(banco_dados)

    arquivos = os.listdir(diretorio_tratados)

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(diretorio_tratados, arquivo)

        if os.path.isfile(caminho_arquivo) and arquivo.endswith(".tsv"):
            df = pd.read_csv(caminho_arquivo, sep='\t', low_memory=False)

            nome_tabela = os.path.splitext(arquivo)[0]
            nome_tabela = nome_tabela.replace(".", "_").replace("-", "_")

            df.to_sql(nome_tabela, conexao, index=False, if_exists='replace')

            logging.info(f"Arquivo {arquivo} salvo como tabela {nome_tabela} no banco de dados.")

            # Remova o arquivo tratado após a carga no banco de dados
            os.remove(caminho_arquivo)

    conexao.close()

    logging.info("Todos os arquivos foram salvos no banco de dados.")

    # CRIAÇÃO DAS TABELAS ANALÍTICAS
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

    queries = [analitico_titulos, analitico_participantes]

    logging.info("Salvando tabelas anlíticas no banco de dados.")

    for query in queries:
        # Defininfo o banco de dados
        banco_dados = "imdb_data.db"
        
        # Conecta ao banco de dados SQLite
        conexao = sqlite3.connect(banco_dados)
        
        # Executa a consulta SQL
        conexao.execute(query)
        
        # Fecha a conexão com o banco de dados
        conexao.close()

    logging.info("Tabelas analíticas criadas com sucesso.")

    print('Fim do processo de ETL')

# Agende a execução do script
schedule.every().day.at("09:00").do(execute_script)

while True:
    schedule.run_pending()
    time.sleep(1)