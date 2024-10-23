# Automação ETL

Este projeto é uma automação de processos ETL (Extract, Transform, Load) utilizando Python para manipular dados do IMDb. O pipeline automatiza a coleta de dados, transformação e carregamento para um banco de dados local.

## Problema de Negócio

Estamos trabalhando em uma empresa de mídia que está montando um novo squad de produto com o objetivo de melhorar as campanhas de marketing baseadas em lançamentos de filmes. Para alcançar esse objetivo, precisamos entender melhor o comportamento de consumo dos usuários de mídia e identificar as tendências de mercado.

Uma das sugestões do time de produto foi explorar os dados do IMDb (Internet Movie Database), que é uma das maiores bases de dados online sobre cinema e a indústria do entretenimento. O IMDb permite que os usuários avaliem produções audiovisuais, e essas avaliações são agregadas em uma nota que reflete a opinião do público.

Neste projeto, começamos sem dados disponíveis para análises, então o primeiro passo foi criar um processo de ETL para extrair, transformar e carregar os dados do IMDb. A preparação desses dados é crucial para que possamos realizar futuras análises e gerar insights que ajudem a equipe de marketing a melhorar suas campanhas.

## Descrição do Projeto

O projeto tem como objetivo automatizar a coleta de dados de filmes do IMDb, realizar transformações necessárias (como limpeza e formatação dos dados) e carregá-los em um banco de dados local (SQLite). Isso é especialmente útil para análise de grandes volumes de dados de forma repetitiva e eficiente.

Este pipeline é dividido em diferentes ciclos (ciclo 2, ciclo 3, ciclo 4 e ciclo 5), cada um representando uma etapa incremental do projeto.

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

## Scripts ETL por Ciclo
- `Ciclo 2:`
 - `Script: automacao-etl-imdb-ciclo-2.py`
 - `Função: Extrai dados do IMDb e realiza transformações iniciais, como limpeza e normalização dos dados.`
- `Ciclo 3:`
 - `Script: automacao-etl-imdb-ciclo-3.py`
 - `Função: Melhora o processo de transformação com novos parâmetros e carrega os dados tratados no banco de dados.`
- `Ciclo 4:`
 - `Script: automacao-etl-imdb-ciclo-4.py`
 - `Função: Realiza novas transformações e integra mais fontes de dados do IMDb.`
- `Ciclo 5:`
 - `Script: automacao-etl-imdb-ciclo-5-dags.py e automacao-etl-imdb-ciclo-5-operadores.py`
 - `Função: Adiciona a execução de DAGs e operadores para orquestrar as etapas do processo ETL, utilizando uma abordagem de Airflow.`


## Estrutura do Diretórios

Abaixo está a estrutura de diretórios do projeto:

- `automacao_etl/`
  - `.git/` - Diretório do Git
  - `.venv/` - Ambiente virtual Python
  - `data/` - Diretório de dados
  - `src/` - Diretório de código-fonte
    - `automacao-etl-imdb-ciclo-2.ipynb` - Notebook do ciclo 2
    - `automacao-etl-imdb-ciclo-2.py` - Script Python do ciclo 2
    - `automacao-etl-imdb-ciclo-3.py` - Script Python do ciclo 3
    - `automacao-etl-imdb-ciclo-4.py` - Script Python do ciclo 4
    - `automacao-etl-imdb-ciclo-5-dags.py` - DAGs do ciclo 5
    - `automacao-etl-imdb-ciclo-5-operadores.py` - Operadores do ciclo 5
    - `etl_imdb.py` - Script principal do processo ETL para o IMDb
    - `imdb_data.db` - Base de dados IMDb
  - `.gitignore` - Arquivo de configuração do Git
  - `README.md` - Documentação do projeto
  - `requirements.txt` - Arquivo de dependências do projeto

## Resultados
- O banco de dados resultante (imdb_data.db) contém as tabelas com as informações transformadas do IMDb.

- Os dados processados podem ser consultados e analisados usando ferramentas de SQL ou bibliotecas Python como pandas.

## Considerações Finais

Esse projeto serve como um exemplo de automação de processos ETL com Python, aplicável a diferentes contextos onde a coleta e o tratamento de grandes volumes de dados são necessários.

