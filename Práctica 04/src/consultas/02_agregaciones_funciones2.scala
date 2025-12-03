import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ 

val spark = SparkSession.builder().getOrCreate()

val registroDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/macrodb")
  .option("dbtable", "registro_infraccion")
  .option("user", "usrmacro")
  .option("password", "passmacro") 
  .load()

val empresaDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/macrodb")
  .option("dbtable", "empresa")
  .option("user", "usrmacro")
  .option("password", "passmacro") 
  .load()

println("--- d) Utilizar groupBy y count (Conteo por Tipo de Servicio) ---")
registroDF.groupBy("tipo_servicio")
  .count()
  .orderBy(col("count").desc) 
  .show(5, false)

println("--- e) Consulta con promedio de una columna (Promedio de multa por transporte) ---")
registroDF.groupBy("tipo_transporte")
  .agg(avg("monto_infraccion").alias("promedio_soles"))
  .show()

println("--- f) Consulta utilizando JOIN (Enriquecer con Nombre de Empresa) ---")

val reporteEmpresasDF = registroDF.join(empresaDF, 
    registroDF("fk_empresa") === empresaDF("id"), 
    "inner"
)

reporteEmpresasDF.select("codigo_papeleta", "placa_vehiculo", "nombre", "monto_infraccion")
  .show(5, false)

println("--- g) Consulta usando funciones (Extraer Año y Mes de la fecha) ---")
registroDF.select(
    col("placa_vehiculo"),
    year(col("fecha_infraccion")).alias("anio"),
    month(col("fecha_infraccion")).alias("mes")
  )
  .filter(col("anio").isNotNull)
  .show(5)
