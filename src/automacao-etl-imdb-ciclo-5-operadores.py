import os
import requests
import shutil
import pandas as pd
import sqlite3
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ExportFilesOperator(BaseOperator):
    template_fields = ['destination_directory', 'file_list']

    @apply_defaults
    def __init__(self, base_url, file_list, destination_directory, *args, **kwargs):
        super(ExportFilesOperator, self).__init__(*args, **kwargs)
        self.base_url = base_url
        self.file_list = file_list
        self.destination_directory = destination_directory

    def execute(self, context):
        os.makedirs(self.destination_directory, exist_ok=True)

        for filename in self.file_list:
            url = f"{self.base_url}{filename}"
            destination_path = os.path.join(self.destination_directory, filename)

            if not os.path.exists(destination_path):
                self.log.info(f"Baixando {filename}...")
                response = requests.get(url, stream=True)

                if response.status_code == 200:
                    with open(destination_path, 'wb') as f:
                        shutil.copyfileobj(response.raw, f)
                    self.log.info(f"{filename} baixado com sucesso!")
                else:
                    self.log.error(f"Falha ao baixar {filename}. Código de status: {response.status_code}")

                del response  # Libera os recursos da resposta
            else:
                self.log.info(f"{filename} já existe. Pulando o download.")

class ProcessFilesOperator(BaseOperator): 
    template_fields = ['source_directory', 'destination_directory', 'file_extension']

    @apply_defaults
    def __init__(self, source_directory, destination_directory, file_extension, *args, **kwargs):
        super(ProcessFilesOperator, self).__init__(*args, **kwargs)
        self.source_directory = source_directory
        self.destination_directory = destination_directory
        self.file_extension = file_extension

    def execute(self, context):
        os.makedirs(self.destination_directory, exist_ok=True)

        for filename in os.listdir(self.source_directory):
            source_path = os.path.join(self.source_directory, filename)

            if os.path.isfile(source_path) and filename.endswith(self.file_extension):
                self.log.info(f"Lendo e processando o arquivo {filename}...")

                df = pd.read_csv(source_path, sep='\t', compression='gzip', low_memory=False)
                df.replace({"\\N": None}, inplace=True)

                destination_path = os.path.join(self.destination_directory, filename[:-3])
                df.to_csv(destination_path, sep='\t', index=False)

                self.log.info(f"Processamento concluído para {filename}. Arquivo processado salvo em {destination_path}")

                # Remove o arquivo de origem após o processamento
                os.remove(source_path)

class SaveToDatabaseOperator(BaseOperator):
    template_fields = ['source_directory', 'database_path']

    @apply_defaults
    def __init__(self, source_directory, database_path, *args, **kwargs):
        super(SaveToDatabaseOperator, self).__init__(*args, **kwargs)
        self.source_directory = source_directory
        self.database_path = database_path

    def execute(self, context):
        conexao = sqlite3.connect(self.database_path)

        for filename in os.listdir(self.source_directory):
            source_path = os.path.join(self.source_directory, filename)

            if os.path.isfile(source_path) and filename.endswith(".tsv"):
                df = pd.read_csv(source_path, sep='\t', low_memory=False)

                nome_tabela = os.path.splitext(filename)[0].replace(".", "_").replace("-", "_")

                df.to_sql(nome_tabela, conexao, index=False, if_exists='replace')

                self.log.info(f"{filename} salvo como tabela {nome_tabela} no banco de dados.")

                # Remove o arquivo processado após salvar no banco de dados
                os.remove(source_path)

        conexao.close()

class CreateAnalyticalTablesOperator(BaseOperator):
    @apply_defaults
    def __init__(self, queries, database_path, *args, **kwargs):
        super(CreateAnalyticalTablesOperator, self).__init__(*args, **kwargs)
        self.queries = queries
        self.database_path = database_path

    def execute(self, context):
        conexao = sqlite3.connect(self.database_path)
        cursor = conexao.cursor()

        for query in self.queries:
            cursor.execute(query)

        conexao.commit()
        conexao.close()
