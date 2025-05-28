# Databricks notebook source
# MAGIC %run /utils/utils

# COMMAND ----------

# MAGIC %run /Users/gabrielsoaresevt@gmail.com/diario_de_bordo 

# COMMAND ----------

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import round, to_date, col
from pyspark.sql.types import DecimalType, IntegerType, DateType
from decimal import Decimal

# COMMAND ----------

def test_leitura_tb_info_transportes(spark):
    """
    Testa se a função leitura_tb_info_transportes retorna um DataFrame com as colunas e linhas esperadas.
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze_layer")

    spark.sql("DROP TABLE IF EXISTS bronze_layer.tb_info_transportes_teste")

    data = [
        Row(DATA_INICIO="2025-05-27 08:30", CATEGORIA="Negocio", LOCAL_INICIO="A", LOCAL_FIM="B", DISTANCIA="12.5", PROPOSITO="Reunião"),
        Row(DATA_INICIO="2025-05-27 09:45", CATEGORIA="Pessoal", LOCAL_INICIO="C", LOCAL_FIM="D", DISTANCIA="7.2", PROPOSITO="Lazer"),
    ]
    df = spark.createDataFrame(data)

    df.write.format("delta").mode("overwrite").saveAsTable("bronze_layer.tb_info_transportes_teste")

    df_lido = spark.table("bronze_layer.tb_info_transportes_teste")
    assert set(df_lido.columns) == {"DATA_INICIO", "CATEGORIA", "LOCAL_INICIO", "LOCAL_FIM", "DISTANCIA", "PROPOSITO"}
    assert df_lido.count() == 2
    assert df_lido is not None

test_leitura_tb_info_transportes(spark)

# COMMAND ----------

def test_tratar_campos_tb_info_transportes(spark):
    """
    Testa se tratar_campos_tb_info_transportes remove espaços, converte tipos e cria a coluna DT_REFE.
    """
    data = [
        Row(DATA_INICIO="05-27-2025 8:30", CATEGORIA=" Negocio ", LOCAL_INICIO="A", LOCAL_FIM="B", PROPOSITO=" Reunião ", DISTANCIA="12.5"),
        Row(DATA_INICIO="05-27-2025 9:45", CATEGORIA="Pessoal", LOCAL_INICIO="C", LOCAL_FIM="D", PROPOSITO="Lazer", DISTANCIA="7.2")
    ]
    df = spark.createDataFrame(data)
    df_trat = tratar_campos_tb_info_transportes(df)
    linhas = df_trat.collect()

    assert " 08:" in linhas[0]["DATA_INICIO"]

    linhas = df_trat.collect()

    assert " 08:" in linhas[0]["DATA_INICIO"]
    assert isinstance(linhas[0]["DISTANCIA"], float) or isinstance(linhas[0]["DISTANCIA"], Decimal)
    assert linhas[0]["CATEGORIA"] == "Negocio"
    assert linhas[0]["PROPOSITO"] == "Reunião"

    assert "DT_REFE" in df_trat.columns

test_tratar_campos_tb_info_transportes(spark)

# COMMAND ----------

def test_gerar_info_corridas_por_dt_ref(spark):
    """
    Testa se gerar_info_corridas_por_dt_ref agrega corretamente por data de referência.
    """
    data = [
        Row(DATA_INICIO="05-27-2025 08:30", CATEGORIA="Negocio", LOCAL_INICIO="A", LOCAL_FIM="B", PROPOSITO="Reunião", DISTANCIA=12.5, DT_REFE="2025-05-27"),
        Row(DATA_INICIO="05-27-2025 09:45", CATEGORIA="Pessoal", LOCAL_INICIO="C", LOCAL_FIM="D", PROPOSITO="Lazer", DISTANCIA=7.2, DT_REFE="2025-05-27"),
        Row(DATA_INICIO="05-28-2025 10:00", CATEGORIA="Negocio", LOCAL_INICIO="E", LOCAL_FIM="F", PROPOSITO="Reunião", DISTANCIA=15.0, DT_REFE="2025-05-28")
    ]
    df = spark.createDataFrame(data)
    df_info = gerar_info_corridas_por_dt_ref(df)
    linhas = df_info.collect()

    assert len(linhas) == 2

    assert "QT_CORR" in df_info.columns
    assert "VL_MAX_DIST" in df_info.columns

test_gerar_info_corridas_por_dt_ref(spark)

# COMMAND ----------

def test_converter_tipos_colunas(spark):
    """
    Testa se converter_tipos_colunas converte os tipos das colunas corretamente.
    """
    data = [
        Row(DT_REFE="2025-05-27", QT_CORR=2, QT_CORR_NEG=1, QT_CORR_PES=1, VL_MAX_DIST=12.5, VL_MIN_DIST=7.2, VL_AVG_DIST=9.9, QT_CORR_REUNI=1, QT_CORR_NAO_REUNI=1),
        Row(DT_REFE="2025-05-28", QT_CORR=1, QT_CORR_NEG=1, QT_CORR_PES=0, VL_MAX_DIST=15.0, VL_MIN_DIST=15.0, VL_AVG_DIST=15.0, QT_CORR_REUNI=1, QT_CORR_NAO_REUNI=0)
    ]
    df = spark.createDataFrame(data)
    df_conv = converter_tipos_colunas(df)
    tipos = dict(df_conv.dtypes)
    assert tipos["DT_REFE"] in ("date", "string")
    assert tipos["QT_CORR"] == "int"
    assert tipos["VL_MAX_DIST"] in ("decimal(10,1)", "double", "float")

test_converter_tipos_colunas(spark)

# COMMAND ----------

def test_criar_tabela_info_corridas_do_dia(spark):
    """
    Testa se criar_tabela_info_corridas_do_dia cria a tabela no schema silver_layer.
    """
    criar_tabela_info_corridas_do_dia(spark)
    tabelas = [x.name for x in spark.catalog.listTables("silver_layer")]
    assert "info_corridas_do_dia" in tabelas

test_criar_tabela_info_corridas_do_dia(spark)

# COMMAND ----------

def test_salvar_tabela_info_corridas_do_dia(spark):
    """
    Testa se salvar_tabela_info_corridas_do_dia salva o DataFrame corretamente como tabela Delta.
    """
    data = [
        Row(DT_REFE="2025-05-27", QT_CORR=2, QT_CORR_NEG=1, QT_CORR_PES=1, VL_MAX_DIST=12.5, VL_MIN_DIST=7.2, VL_AVG_DIST=9.9, QT_CORR_REUNI=1, QT_CORR_NAO_REUNI=1),
    ]
    df = spark.createDataFrame(data)

    df = df\
        .withColumn("DT_REFE", to_date(col("DT_REFE")))\
        .withColumn("QT_CORR", col("QT_CORR").cast("integer"))\
        .withColumn("QT_CORR_NEG", col("QT_CORR_NEG").cast("integer"))\
        .withColumn("QT_CORR_PES", col("QT_CORR_PES").cast("integer"))\
        .withColumn("VL_MAX_DIST", col("VL_MAX_DIST").cast(DecimalType(10,1)))\
        .withColumn("VL_MIN_DIST", col("VL_MIN_DIST").cast(DecimalType(10,1)))\
        .withColumn("VL_AVG_DIST", col("VL_AVG_DIST").cast(DecimalType(10,1)))\
        .withColumn("QT_CORR_REUNI", col("QT_CORR_REUNI").cast("integer"))\
        .withColumn("QT_CORR_NAO_REUNI", col("QT_CORR_NAO_REUNI").cast("integer"))

    criar_tabela_info_corridas_do_dia(spark)

    salvar_tabela_info_corridas_do_dia(df)
    result = spark.table("silver_layer.info_corridas_do_dia")
    assert result.count() >= 1

test_salvar_tabela_info_corridas_do_dia(spark)

# COMMAND ----------

def test_get_spark_session(spark):
    """
    Testa se get_spark_session retorna uma instância de SparkSession.
    """
    assert isinstance(spark, SparkSession)

test_get_spark_session(spark)

# COMMAND ----------

if __name__ == "__main__":
    spark = get_spark_session()

    test_leitura_tb_info_transportes(spark)
    test_tratar_campos_tb_info_transportes(spark)
    test_gerar_info_corridas_por_dt_ref(spark)
    test_converter_tipos_colunas(spark)
    test_criar_tabela_info_corridas_do_dia(spark)
    test_salvar_tabela_info_corridas_do_dia(spark)
    test_get_spark_session(spark)
    print("Todos os testes passaram!")