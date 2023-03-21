// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("padron_scala").getOrCreate()
val padronFile = "/FileStore/tables/estadisticas202212-1.csv"

// COMMAND ----------

//cargamos el csv en un DF
val padronDF = spark.read
  .format("csv")
  .option("delimiter", ";")
  .option("inferSchema", "true")
  .option("header", "true")
  .load(padronFile)

// COMMAND ----------

padronDF.show()

// COMMAND ----------

//Eliminar espacios en algunas columnas
import org.apache.spark.sql.functions._
val padronMod = padronDF
  .withColumn("desc_distrito", trim(col("desc_distrito"))) //forma col
  .withColumn("desc_barrio", trim($"desc_barrio")) //forma $

padronMod.show(5, false)

// COMMAND ----------

//enum los barrios diferentes
val barriosUniq = padronDF.select("desc_barrio").distinct().show()

// COMMAND ----------

padronDF.createOrReplaceTempView("padron")
spark.sql("SELECT COUNT(DISTINCT(desc_barrio)) FROM padron").show()

// COMMAND ----------

//Crear una nueva columna con la long de desc_distrito y llamarlo longitud
val padronMod1 = padronMod.withColumn("longitud", length(col("desc_distrito")))
padronMod1.show()

// COMMAND ----------

//Crear nueva columna que muestr el valor 5 para cada uno de los registros de la tabla
val padronMod2 = padronMod1.withColumn("valor_5", lit(5)) //creamos un literal con valor constante
padronMod2.show()

// COMMAND ----------

//Borrar la col valor_5
val padronMod3 = padronMod2.drop($"valor_5")
padronMod3.show()

// COMMAND ----------

//repartition() creates a specified number of partitions in memory. The partitionBy () will write files to disk for each memory partition and partition column
val padronMod4 = padronMod3.repartition(col("desc_distrito"), col("desc_barrio"))
padronMod4.show()

// COMMAND ----------

//almacenarlo en el cache
padronMod4.persist()

// COMMAND ----------

/*

Lanza una consulta contra el DF resultante en la que muestre el número total de
"espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres"
para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en
aparecer en el show. Los resultados deben estar ordenados en orden de más a menos
según la columna "extranjerosmujeres" y desempatarán por la columna
"extranjeroshombres".

*/


val query1 = padronMod4
  .groupBy($"DESC_DISTRITO", $"DESC_BARRIO")
  .agg(
    sum($"espanoleshombres").alias("sum_espanoleshombres"),
    sum($"espanolesmujeres").alias("sum_espanolesmujeres"),
    sum($"extranjeroshombres").alias("sum_extranjeroshombres"),
    sum($"extranjerosmujeres").alias("sum_extranjerosmujeres"),
  )
  .orderBy(desc("sum_extranjerosmujeres"), desc("sum_extranjeroshombres"))
query1.show()

// COMMAND ----------

//elimiar el registro del cache
padronMod4.unpersist()

// COMMAND ----------

/*
Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con
DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres"
residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a
través de las columnas en común.
*/

val newPadronDF = padronDF
  .groupBy($"DESC_BARRIO", $"DESC_DISTRITO")
  .agg(
    sum($"espanoleshombres").alias("espanoleshombres")
  )

newPadronDF.show()

// COMMAND ----------

val padronJoin = padronDF.join(newPadronDF, Seq("DESC_BARRIO", "DESC_DISTRITO"), "inner")
padronJoin.show()

// COMMAND ----------

//Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).
import org.apache.spark.sql.expressions.Window

// Particionar el DataFrame padronDF por la columna "id"
val partitionedDf1 = padronDF
  .withColumn("barrio_column", col("DESC_BARRIO"))
  .withColumn("distrito_column", col("DESC_DISTRITO"))
  .withColumn("row_num", row_number().over(Window.partitionBy("barrio_column", "distrito_column").orderBy("DESC_BARRIO", "DESC_DISTRITO")))

// Particionar el DataFrame df2 por la columna "id"
val partitionedDf2 = newPadronDF
  .withColumn("barrio_column", col("DESC_BARRIO"))
  .withColumn("distrito_column", col("DESC_DISTRITO"))
  .withColumn("row_num", row_number().over(Window.partitionBy("barrio_column", "distrito_column").orderBy("DESC_BARRIO", "DESC_DISTRITO")))

// Unir los DataFrames particionados por la columna "partition_column" y la columna "row_num"
val joinedDf = partitionedDf1.join(partitionedDf2, Seq("barrio_column", "distrito_column"), "inner").drop("barrio_column", "distrito_column")
joinedDf.show()


// COMMAND ----------

/*
Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que
contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y
en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente
CENTRO, BARAJAS y RETIRO y deben figurar como columnas . El aspecto debe ser similar a
este:

*/

val pivotDF = padronMod
  .filter($"DESC_DISTRITO".isin("CENTRO", "BARAJAS", "RETIRO"))
  .groupBy($"COD_EDAD_INT")
  .pivot($"DESC_DISTRITO")
  .agg(sum($"espanolesmujeres"))
  .orderBy($"COD_EDAD_INT")

pivotDF.show()

// COMMAND ----------

/*
Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje
de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa
cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la
condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.

*/

val totalDF = pivotDF
  .agg(
    sum($"BARAJAS").alias("sumaBarajas"),
    sum($"CENTRO").alias("sumaCentro"),
    sum($"RETIRO").alias("sumaRetiro")
      )
val porcentajeDF = pivotDF
  .withColumn("COD_EDAD_INT", $"COD_EDAD_INT")
  .withColumn("BARAJAS", $"BARAJAS")
  .withColumn("CENTRO", $"CENTRO".cast("double"))
  .withColumn("RETIRO", $"RETIRO".cast("double"))


pivotDF.printSchema()
totalDF.printSchema()
porcentajeDF.printSchema()
totalDF.show()
porcentajeDF.show()

// COMMAND ----------

/*

Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un
directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba
que es la esperada -> 

*/
padronDF.write
  .partitionBy("desc_distrito","desc_barrio")
  .option("header", "true")
  .option("delimiter", ";")
  .mode("overwrite")
  .csv("/FileStore/tables/padronParticionadoCSV")


// COMMAND ----------

/*
Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el
resultado anterior.
*/

padronDF.write
  .partitionBy("desc_distrito","desc_barrio")
  .option("header", "true")
  .option("delimiter", ";")
  .mode("overwrite")
  .parquet("/FileStore/tables/padronParticionadoParquet")

// COMMAND ----------


