import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// 1. Cargar la tabla
val registroDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/macrodb")
  .option("dbtable", "registro_infraccion")
  .option("user", "usrmacro")
  .option("password", "passmacro")
  .load()

println("--- a) Proyección: Mostrar columnas específicas ---")
registroDF.select("placa_vehiculo", "monto_infraccion").show(5)

println("--- b) Filtro: Multas mayores a 500 soles ---")
registroDF.filter(registroDF("monto_infraccion") > 500)
  .select("codigo_papeleta", "placa_vehiculo", "monto_infraccion") 
  .show(5)

println("--- c) Ordenamiento: Multas más caras primero ---")
registroDF.orderBy(registroDF("monto_infraccion").desc)
  .select("codigo_papeleta", "fecha_infraccion", "monto_infraccion")
  .show(5)
