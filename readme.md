# 🎵 Music Analytics Data Lake (DuckDB)

## 📋 Sobre o Projeto
Projeto prático de **Engenharia de Dados**, focado na construção de um Data Lake local robusto e performático.
O objetivo foi ingerir dados brutos de plataformas de streaming (Spotify e YouTube), processá-los seguindo a **Arquitetura Medalhão (Medallion Architecture)** e preparar as bases para modelos de Machine Learning.

Diferente de bancos tradicionais, este projeto utiliza **DuckDB**, um banco de dados OLAP in-process, ideal para processamento analítico veloz sem a necessidade de gerenciar servidores pesados.

## 🛠️ Tecnologias Utilizadas
* **Linguagem:** Python 3.10+
* **Processamento & SQL:** DuckDB (OLAP)
* **Manipulação de Dados:** Pandas
* **Armazenamento:** Parquet (Compressão Snappy)
* **Ingestão:** Kaggle API
* **Machine Learning:** Scikit-learn (para clusterização/análise)

## 🏗️ Arquitetura do Pipeline
O fluxo de dados foi organizado em camadas lógicas para garantir a qualidade e a rastreabilidade:

1.  **Extract (Bronze Layer):**
    * Ingestão automatizada via API do Kaggle.
    * Armazenamento dos dados brutos (`raw`) em formato local.
2.  **Transform (Silver Layer):**
    * Limpeza de dados, remoção de duplicatas e tipagem.
    * Conversão para formato **.parquet** (colunar) para otimizar leitura.
    * Script: `elt_duckdb.py`
3.  **Load/Aggregate (Gold Layer):**
    * Criação de visões de negócio (ex: Perfil das Bandas).
    * Dados prontos para consumo por ferramentas de BI ou modelos de ML.
4.  **Machine Learning:**
    * Consumo da camada Gold para análise exploratória e algoritmos de recomendação/classificação.

## 🗂️ Estrutura do Data Lake
```text
datalake/
├── bronze/  # Dados crus (Raw Data)
├── silver/  # Dados limpos e tipados (Parquet)
└── gold/    # Tabelas agregadas de negócio (Parquet)
 ```

## 🚀 Como Executar
Para reproduzir este projeto localmente, siga os passos abaixo:

1. **Clone o repositório**
   ```bash
   git clone [https://github.com/Consolucas/music-analytics-data-lake.git]
   cd NOME_DO_PROJETO
   ```

2. **Configure o Ambiente Virtual**
   É recomendado usar um ambiente virtual para não conflitar bibliotecas.
   ```bash
   # Windows
   python -m venv venv
   .\venv\Scripts\activate

   # Linux/Mac
   source venv/bin/activate
   ```

3. **Instale as Dependências**
   ```bash
   pip install -r requirements.txt
   ```

4. **Execute o Pipeline**
   Rode os scripts na ordem lógica de processamento de dados:

   * **Passo 1: Ingestão (Bronze)**
     Baixa o dataset do Kaggle automaticamente para a pasta bronze.
     ```bash
     python scripts/importa_kaggle.py
     ```

   * **Passo 2: Transformação (Silver & Gold)**
     Processa os dados com DuckDB, limpa e salva em Parquet.
     ```bash
     python scripts/elt_duckdb.py
     ```

   * **Passo 3: Análise**
     Executa a lógica de análise ou modelo de Machine Learning.
     ```bash
     python scripts/ml_musica.py
     ```

## 💾 Fonte dos Dados
Este projeto utiliza o dataset público **Spotify and Youtube**, disponível no Kaggle, que contém métricas de popularidade de músicas em ambas as plataformas.

* **Dataset Original:** [Spotify and Youtube - Kaggle](https://www.kaggle.com/datasets/salvatorerastelli/spotify-and-youtube)

---
*Desenvolvido por Lucas Consolo*