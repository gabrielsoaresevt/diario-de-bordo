# Databricks notebook source
from pyspark.sql import SparkSession

def get_spark_session():
    """
    Cria e retorna uma instância do SparkSession configurada para a aplicação 'Info Corridas do Dia'.

    Retorna:
        SparkSession: Instância ativa do SparkSession para execução de operações Spark.
    """
    spark = SparkSession.builder \
        .appName("Info Corridas do Dia") \
        .getOrCreate()

    return spark

def criar_tabela_info_corridas_do_dia(spark):
  """
    Cria a tabela 'info_corridas_do_dia' no schema 'silver_layer' caso ela ainda não exista, 
    utilizando o formato Delta. Define a estrutura e os comentários das colunas conforme o modelo de dados esperado.

    Parâmetros:
        spark (SparkSession): Instância ativa do SparkSession utilizada para executar o comando SQL de criação da tabela.
  """
  spark.sql("""
      CREATE TABLE IF NOT EXISTS silver_layer.info_corridas_do_dia (
        DT_REFE DATE COMMENT 'Data de referência.',
        QT_CORR INT COMMENT 'Quantidade de corridas.',
        QT_CORR_NEG INT COMMENT 'Quantidade de corridas com a categoria “Negócio”.',
        QT_CORR_PES INT COMMENT 'Quantidade de corridas com a categoria “Pessoal”.',
        VL_MAX_DIST DECIMAL(10,1) COMMENT 'Maior distância percorrida por uma corrida.',
        VL_MIN_DIST DECIMAL(10,1) COMMENT 'Menor distância percorrida por uma corrida.',
        VL_AVG_DIST DECIMAL(10,1) COMMENT 'Média das distâncias percorridas.',
        QT_CORR_REUNI INT COMMENT 'Quantidade de corridas com o propósito de "Reunião".',
        QT_CORR_NAO_REUNI INT COMMENT 'Quantidade de corridas com o propósito declarado e diferente de "Reunião".'
      )
      USING DELTA
  """)

def salvar_tabela_info_corridas_do_dia(df_final):
  """
    Salva o DataFrame final no formato Delta, sobrescrevendo a tabela 'silver_layer.info_corridas_do_dia'.

    Parâmetros:
        df_final (DataFrame): DataFrame Spark contendo os dados finais agregados e tratados para persistência na camada Silver.
  """
  df_final.write.format("delta").mode("overwrite").saveAsTable("silver_layer.info_corridas_do_dia")