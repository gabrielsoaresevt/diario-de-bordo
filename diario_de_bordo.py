# Databricks notebook source
# MAGIC %md
# MAGIC # Projeto: Pipeline de Processamento de Corridas - Databricks & PySpark
# MAGIC
# MAGIC Este projeto contém um pipeline de processamento de dados de corridas, desenvolvido em Databricks utilizando PySpark e Delta Lake. O objetivo é ler, tratar, agregar e persistir informações de corridas, facilitando análises e relatórios.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Índice
# MAGIC
# MAGIC 1. [Estrutura do Projeto](#-estrutura-do-projeto)
# MAGIC 2. [Fluxo do Pipeline](#fluxo-do-pipeline)
# MAGIC 3. [Como Executar](#-como-executar)
# MAGIC 4. [Testes](#testes)
# MAGIC 5. [Observações](#observações)
# MAGIC 6. [Autor](#autor)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📁 Estrutura do Projeto
# MAGIC
# MAGIC - **diario_de_bordo**: Pipeline ETL completo, do bronze ao silver.
# MAGIC - **utils**: Funções utilitárias e helpers para o pipeline.
# MAGIC - **tests_app**: Testes unitários para garantir a qualidade das funções.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Fluxo do Pipeline
# MAGIC
# MAGIC 1. **Leitura**  
# MAGIC    Lê dados da tabela `bronze_layer.tb_info_transportes` (arquivo `info_transportes` no Databricks).
# MAGIC
# MAGIC 2. **Tratamento**  
# MAGIC    Ajusta tipos, remove espaços e cria colunas derivadas.
# MAGIC
# MAGIC 3. **Agregação**  
# MAGIC    Gera estatísticas diárias das corridas (quantidades, distâncias, propósitos).
# MAGIC
# MAGIC 4. **Conversão de Tipos**  
# MAGIC    Garante que cada coluna tenha o tipo correto para análise e persistência.
# MAGIC
# MAGIC 5. **Persistência**  
# MAGIC    Cria e salva a tabela `silver_layer.info_corridas_do_dia` em formato Delta.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Como Executar
# MAGIC
# MAGIC 1. Faça upload do arquivo `info_transportes` para o Databrickse nomeie a tabela para `tb_info_transportes`.
# MAGIC 2. Faça upload dos arquivos `utils` e `tests_app` e importe-os para o `Workspace` dentro dos diretórios `/utils/utils` e `tests/tests_app`
# MAGIC 3. Execute o notebook `diario_de_bordo` para rodar o pipeline.
# MAGIC 4. Consulte os resultados na tabela `silver_layer.info_corridas_do_dia`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Testes
# MAGIC
# MAGIC Os testes unitários estão em `tests_app` e cobrem:
# MAGIC - Leitura de dados
# MAGIC - Tratamento de campos
# MAGIC - Agregações
# MAGIC - Conversão de tipos
# MAGIC - Criação e persistência de tabelas
# MAGIC
# MAGIC Recomenda-se rodar os testes para garantir a integridade do pipeline.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Observações
# MAGIC
# MAGIC - Projeto desenvolvido e testado no Databricks Community Edition.
# MAGIC - Utiliza PySpark, Delta Lake e SQL do Databricks.
# MAGIC - Estrutura modular para facilitar manutenção e evolução.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Autor
# MAGIC
# MAGIC Gabriel Soares Evangelista - [@gabrielsoaresevt](https://www.linkedin.com/in/gabriel-soares-evangelista)

# COMMAND ----------

# MAGIC %md
# MAGIC Importar notebook de utils

# COMMAND ----------

# MAGIC %run /utils/utils

# COMMAND ----------

# MAGIC %md
# MAGIC Criar Databases camadas Bronzes e Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze_layer;
# MAGIC CREATE DATABASE IF NOT EXISTS silver_layer;

# COMMAND ----------

# MAGIC %md
# MAGIC Importações necessárias

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import round, to_date, regexp_replace, col, count, when, max, min, avg, trim
from pyspark.sql.types import DateType, IntegerType, DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC Diário de Bordo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC             DATA_INICIO, 
# MAGIC             CATEGORIA, 
# MAGIC             LOCAL_INICIO, 
# MAGIC             LOCAL_FIM, 
# MAGIC             PROPOSITO, 
# MAGIC             DISTANCIA 
# MAGIC             FROM bronze_layer.tb_info_transportes

# COMMAND ----------

def leitura_tb_info_transportes(spark):
    """
    Realiza a leitura da tabela 'tb_info_transportes' no schema 'bronze_layer' utilizando uma consulta SQL via Spark.

    Parâmetros:
        spark (SparkSession): Instância ativa do SparkSession utilizada para executar a consulta SQL.

    Retorna:
        DataFrame: DataFrame Spark contendo as colunas DATA_INICIO, CATEGORIA, LOCAL_INICIO, LOCAL_FIM, PROPOSITO e DISTANCIA extraídas da tabela.
    """
    
    df_info_transportes = spark\
        .sql("""
            SELECT 
            DATA_INICIO, 
            CATEGORIA, 
            LOCAL_INICIO, 
            LOCAL_FIM, 
            PROPOSITO, 
            DISTANCIA 
            FROM bronze_layer.tb_info_transportes
        """)
    
    return df_info_transportes

# COMMAND ----------

def tratar_campos_tb_info_transportes(df_info_transportes):
    """
    Realiza o tratamento dos campos do DataFrame de informações de transportes, realizando ajustes de formatação, tipos de dados e criação de colunas derivadas.

    Parâmetros:
        df_info_transportes (DataFrame): DataFrame Spark contendo os dados brutos da tabela de informações de transportes.

    Retorna:
        DataFrame: DataFrame Spark com os tratamentos aplicados
    """
    
    df_trat_info_transportes = df_info_transportes\
        .withColumn("DATA_INICIO", regexp_replace(col("DATA_INICIO"), r" (\d):", " 0$1:"))\
        .withColumn("DISTANCIA", col("DISTANCIA").cast(DecimalType(10,1)))\
        .withColumn("CATEGORIA", trim(col("CATEGORIA"))) \
        .withColumn("PROPOSITO", trim(col("PROPOSITO"))) \
        .withColumn("DT_REFE", to_date(col("DATA_INICIO"), "MM-dd-yyyy HH:mm"))

    return df_trat_info_transportes

# COMMAND ----------

def gerar_info_corridas_por_dt_ref(df_trat_info_transportes):
    """
    Gera estatísticas agregadas sobre as corridas a partir do DataFrame tratado de informações de transportes.

    Parâmetros:
        df_trat_info_transportes (DataFrame): DataFrame Spark contendo os dados de transportes já tratados e com a coluna "DT_REFE".

    Retorna:
        DataFrame: DataFrame Spark agrupado por "DT_REFE", contendo as agreções necessárias
    """
    
    df_info_corridas_do_dia = df_trat_info_transportes.groupBy("DT_REFE").agg(
        count("*").alias("QT_CORR"),
        count(when(col("CATEGORIA") == "Negocio", True)).alias("QT_CORR_NEG"),
        count(when(col("CATEGORIA") == "Pessoal", True)).alias("QT_CORR_PES"),
        round(max("DISTANCIA"), 1).alias("VL_MAX_DIST"),
        round(min("DISTANCIA"), 1).alias("VL_MIN_DIST"),
        round(avg("DISTANCIA"), 1).alias("VL_AVG_DIST"),
        count(when(col("PROPOSITO") == "Reunião", True)).alias("QT_CORR_REUNI"),
        count(when((col("PROPOSITO").isNotNull()) & (col("PROPOSITO") != "Reunião"), True)).alias("QT_CORR_NAO_REUNI")
    )

    return df_info_corridas_do_dia

# COMMAND ----------

def converter_tipos_colunas(df_info_corridas_do_dia):
    """
    Converte os tipos de dados das colunas do DataFrame de informações das corridas para os tipos apropriados.

    Parâmetros:
        df_info_corridas_do_dia (DataFrame): DataFrame Spark contendo dados agregados sobre as corridas por data de referência.

    Retorna:
        DataFrame: DataFrame Spark com as colunas convertidas para os seguintes tipos:
            - "DT_REFE": DateType
            - "QT_CORR", "QT_CORR_NEG", "QT_CORR_PES", "QT_CORR_REUNI", "QT_CORR_NAO_REUNI": IntegerType
            - "VL_MAX_DIST", "VL_MIN_DIST", "VL_AVG_DIST": DecimalType(10, 1)
    """
    
    colunas_data_types = {
        "DT_REFE": DateType(),
        "QT_CORR": IntegerType(),
        "QT_CORR_NEG": IntegerType(),
        "QT_CORR_PES": IntegerType(),
        "VL_MAX_DIST": DecimalType(10, 1),
        "VL_MIN_DIST": DecimalType(10, 1),
        "VL_AVG_DIST": DecimalType(10, 1),
        "QT_CORR_REUNI": IntegerType(),
        "QT_CORR_NAO_REUNI": IntegerType()
    }

    df_final_info_corridas = df_info_corridas_do_dia
    for coluna, tipo in colunas_data_types.items():
        df_final_info_corridas = df_final_info_corridas.withColumn(coluna, col(coluna).cast(tipo))

    return df_final_info_corridas

# COMMAND ----------

# MAGIC %md
# MAGIC Função principal

# COMMAND ----------

def main (spark):
    """
    Executa o pipeline completo de processamento dos dados de transportes, incluindo leitura, tratamento, agregação, conversão de tipos e persistência dos resultados em tabela Delta.

    Parâmetros:
        spark (SparkSession): Instância ativa do SparkSession utilizada para as operações de leitura, processamento e escrita dos dados.

    Fluxo de execução:
        1. Lê os dados da tabela 'tb_info_transportes' do schema 'bronze_layer'.
        2. Realiza o tratamento dos campos do DataFrame lido.
        3. Gera as informações agregadas das corridas por data de referência.
        4. Converte os tipos das colunas do DataFrame agregado.
        5. Cria a tabela 'info_corridas_do_dia' no schema 'silver_layer' se ainda não existir.
        6. Salva o DataFrame final como tabela Delta no schema 'silver_layer'.
    """
    
    df_info_transportes = leitura_tb_info_transportes(spark)
    df_trat_info_transportes = tratar_campos_tb_info_transportes(df_info_transportes)
    df_info_corridas_do_dia = gerar_info_corridas_por_dt_ref(df_trat_info_transportes)
    df_final_info_corridas = converter_tipos_colunas(df_info_corridas_do_dia)

    criar_tabela_info_corridas_do_dia(spark)
    salvar_tabela_info_corridas_do_dia(df_final_info_corridas)

# COMMAND ----------

if __name__ == "__main__":
    spark = get_spark_session()
    
    main(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC Salvar resultado final no Banco de Dados

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM silver_layer.info_corridas_do_dia

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE FORMATTED silver_layer.info_corridas_do_dia