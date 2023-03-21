// Databricks notebook source
import org.apache.spark.sql.SparkSession
//Create spark session
val spark = SparkSession
  .builder()
  .appName("ejPadron")
  .getOrCreate()

// COMMAND ----------

//1.1 create db datos_padron
spark.sql("set hive.cli.print.header=true")
spark.sql("CREATE DATABASE IF NOT EXISTS datos_padron")

// COMMAND ----------

//existing databases and use the datos_padron one
spark.sql("SHOW DATABASES").show()
spark.sql("USE datos_padron")

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS padron_txt

// COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")


// COMMAND ----------

//1.2 create table padron_txt and load CSV file

spark.sql("""CREATE TABLE padron_txt(cod_distrito STRING, 
                                   desc_distrito STRING, 
                                   cod_dist_barrio STRING, 
                                   desc_barrio STRING, 
                                   cod_barrio STRING,
                                   cod_dist_seccion STRING,
                                   cod_seccion STRING,
                                   cod_edad_int STRING,
                                   espanoleshombres STRING, 
                                   espanolesmujeres STRING, 
                                   extranjeroshombres STRING, 
                                   extranjerosmujeres STRING, 
                                   fx_carga STRING,
                                   fx_datos_ini STRING,
                                   fx_datos_fin STRING)
                                   ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
                                   tblproperties('skip.header.line.count'='1')
                                   STORED AS TEXTFILE""")

spark.sql("SHOW TABLES").show()

spark.sql("DESC padron_txt").show()


// COMMAND ----------

//import the csv using load
spark.sql("LOAD DATA INPATH '/FileStore/tables/estadisticas202212.csv' INTO TABLE padron_txt")


// COMMAND ----------

val tb = spark.sql("SELECT * FROM padron_txt WHERE cod_distrito != 'COD_DISTRITO'").show(5)


// COMMAND ----------

//example
val ej = spark.sql("SELECT trim(substr(desc_distrito, 2, 20)) AS pb, desc_distrito FROM padron_txt")
ej.show(10, false)


// COMMAND ----------

//1.3 CTAS, trim 
spark.sql("DROP TABLE IF EXISTS padron_txt_ctas")
spark.sql("""CREATE TABLE padron_txt_ctas AS SELECT 
  TRIM(REPLACE(cod_distrito, '\"', '')) AS cod_distrito, 
  TRIM(REPLACE(desc_distrito, '\"', '')) AS desc_distrito,
  TRIM(REPLACE(cod_dist_barrio, '\"', '')) AS cod_dist_barrio,
  TRIM(REPLACE(desc_barrio, '\"', '')) AS desc_barrio,
  TRIM(REPLACE(cod_barrio, '\"', '')) AS cod_barrio,
  TRIM(REPLACE(cod_dist_seccion, '\"', '')) AS cod_dist_seccion,
  TRIM(REPLACE(cod_seccion, '\"', '')) AS cod_seccion,
  TRIM(REPLACE(cod_edad_int, '\"', '')) AS cod_edad_int,
  TRIM(REPLACE(espanoleshombres, '\"', '')) AS espanoleshombres,
  TRIM(REPLACE(espanolesmujeres, '\"', '')) AS espanolesmujeres,
  TRIM(REPLACE(extranjeroshombres, '\"', '')) AS extranjeroshombres,
  TRIM(REPLACE(extranjerosmujeres, '\"', '')) AS extranjerosmujeres,
  TRIM(REPLACE(fx_carga, '\"', '')) AS fx_carga,
  TRIM(REPLACE(fx_datos_ini, '\"', '')) AS fx_datos_ini,
  TRIM(REPLACE(fx_datos_fin, '\"', '')) AS fx_datos_fin
  FROM padron_txt""")

// COMMAND ----------

spark.sql("SELECT * FROM padron_txt_ctas").show(5, false)

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS padron_txt_mod")
spark.sql("""CREATE TABLE padron_txt_mod AS SELECT 
  cod_distrito AS cod_distrito, 
  desc_distrito AS desc_distrito,
  cod_dist_barrio AS cod_dist_barrio,
  desc_barrio AS desc_barrio,
  cod_barrio AS cod_barrio,
  cod_dist_seccion AS cod_dist_seccion,
  cod_seccion AS cod_seccion,
  cod_edad_int AS cod_edad_int,
  CASE WHEN LEN(espanoleshombres)== 0 THEN 0 ELSE espanoleshombres END AS espanoleshombres,
  CASE WHEN LEN(espanolesmujeres)== 0 THEN 0 ELSE espanolesmujeres END AS espanolesmujeres,
  CASE WHEN LEN(extranjeroshombres)== 0 THEN 0 ELSE extranjeroshombres END AS extranjeroshombres,
  CASE WHEN LEN(extranjerosmujeres)== 0 THEN 0 ELSE extranjerosmujeres END AS extranjerosmujeres,
  fx_carga AS fx_carga,
  fx_datos_ini AS fx_datos_ini,
  fx_datos_fin AS fx_datos_fin
  FROM padron_txt_ctas""")

// COMMAND ----------

spark.sql("SELECT * FROM padron_txt_mod").show(10, false)

// COMMAND ----------

//2.2 Parquet
//snappy aporta eficiencia a la hora de comprimir y descomprimir -> por defecto esta habilitado
//Formato parquet de la tabla padron_txt (sin cambiar nada)
spark.sql("""CREATE TABLE padron_parquet 
          STORED AS PARQUET
          AS SELECT *
          FROM padron_txt""")

// COMMAND ----------

spark.sql("SELECT * FROM padron_parquet").show(5, false)

// COMMAND ----------

//2.3
//Formato parquet de la tabla padron_txt_mod (los cambios realizados han sido quitar las dobles comillas y espacios)
spark.sql("""CREATE TABLE padron_parquet_mod
          STORED AS PARQUET
          AS SELECT * 
          FROM padron_txt_mod""")

// COMMAND ----------

spark.sql("SELECT * FROM padron_parquet_mod").show(5, false)

// COMMAND ----------

// Impala
//3.1 y 3.2
//Diferencias entre impala y hive
//Impala es mas rapido que hive, ya que impala procesa los datos directamente en HDFS y hive al usar MapReduce en el procesamiento de datos es más lento
//Impala esta diseño para la baja latencia(destaca en real-time) mientras que hive procesamiento en batch
//Impala requiere más recursos de memoria y cpu
//Dependiendo de lo que vayas a usar puede ser más util uno u otro, por ejemplo impala para baja latencia y dataset pequeños, y en caso contrario hive

// COMMAND ----------

//3.3 - 3.4 INVALIDATE METADA -> asegurar que impala metadata up to date y sincronizado con cualquier cambio realizado y como consecuenicia fuerza a impala a recargar los metadatos
//INVALIDATE METADATA datos_padron.*

// COMMAND ----------

//3.5
val query1 = """
  SELECT desc_distrito, desc_barrio, SUM(espanoleshombres), SUM(espanolesmujeres), SUM(extranjeroshombres), SUM(extranjerosmujeres)
  FROM padron_txt_mod
  GROUP BY desc_distrito, desc_barrio
"""
spark.sql(query1).show(false)

// COMMAND ----------

val query2 = """
  SELECT desc_distrito, desc_barrio, SUM(espanoleshombres), SUM(espanolesmujeres), SUM(extranjeroshombres), SUM(extranjerosmujeres)
  FROM padron_parquet_mod
  GROUP BY desc_distrito, desc_barrio
"""
spark.sql(query2).show(false)

// COMMAND ----------


