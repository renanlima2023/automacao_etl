# Automação ETL

Este projeto é uma automação de processos ETL (Extract, Transform, Load) utilizando Python para manipular dados do IMDb. O pipeline automatiza a coleta de dados, transformação e carregamento para um banco de dados local.

## Descrição do Projeto

O objetivo deste projeto é processar dados extraídos do IMDb, realizar transformações necessárias e carregá-los em um banco de dados SQLite. Esse processo é parte de um ciclo de automação ETL, ideal para cenários onde grandes quantidades de dados precisam ser tratadas de forma eficiente e repetitiva.

## Pré-requisitos

Antes de começar, certifique-se de que você tenha as seguintes ferramentas instaladas:

- Python 3.8+ (Recomendado)
- Git
- Virtualenv

## Tecnologias Utilizadas

As principais tecnologias e bibliotecas utilizadas neste projeto incluem:

- **Python**: Linguagem principal para a automação ETL.
- **SQLite**: Banco de dados relacional onde os dados extraídos são armazenados.
- **Pandas**: Biblioteca de manipulação e análise de dados.
- **SQLAlchemy**: ORM usado para interação com o banco de dados.
- **Requests**: Para fazer requisições HTTP e consumir a API do IMDb.

## Instalação

Siga estes passos para configurar o projeto em sua máquina local:

1. Clone o repositório:
   ```
   git clone git@github.com:renanlima2023/-automacao_etl.git
   ```

2. Entre no diretório do projeto:
   ```
   cd -automacao_etl
   ```

3. Crie um ambiente virtual:
   ```
   python -m venv .venv
   ```

4. Ative o ambiente virtual:
   - No Windows:
     ```
     .venv\Scripts\activate
     ```
   - No macOS e Linux:
     ```
     source .venv/bin/activate
     ```

5. Instale as dependências:
   ```
   pip install -r requirements.txt
   ```

## Executando o projeto

Para rodar o projeto, execute o seguinte comando no terminal: python src/etl_imdb.py

## Estrutura do Projeto

A estrutura de diretórios do projeto é a seguinte:
|- automacao_etl/
   |- .git/                  # Diretório do Git
   |- .venv/                 # Ambiente virtual Python
   |- data/                  # Diretório de dados
   |- src/                   # Diretório de código-fonte
      |- automacao-etl-imdb-ciclo-2.ipynb  # Notebook do ciclo 2
      |- automacao-etl-imdb-ciclo-2.py     # Script Python do ciclo 2
      |- automacao-etl-imdb-ciclo-3.py     # Script Python do ciclo 3
      |- automacao-etl-imdb-ciclo-4.py     # Script Python do ciclo 4
      |- automacao-etl-imdb-ciclo-5-dags.py    # DAGs do ciclo 5
      |- automacao-etl-imdb-ciclo-5-operadores.py  # Operadores do ciclo 5
      |- etl_imdb.py             # Script principal do processo ETL para o IMDb
      |- imdb_data.db            # Base de dados IMDb
   |- .gitignore             # Arquivo de configuração Git
   |- README.md              # Documentação do projeto
   |- requirements.txt       # Arquivo de dependências do projeto
 ### Fim

# automacao_etl
