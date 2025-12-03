import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("ConsultasSQL").getOrCreate()

val registroDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/macrodb")
  .option("dbtable", "registro_infraccion")
  .option("user", "usrmacro")
  .option("password", "passmacro")
  .load()

val empresasDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/macrodb")
  .option("dbtable", "empresa")
  .option("user", "usrmacro")
  .option("password", "passmacro")
  .load()

val tipoInfraccionDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/macrodb")
  .option("dbtable", "tipo_infraccion")
  .option("user", "usrmacro")
  .option("password", "passmacro")
  .load()

registroDF.createOrReplaceTempView("v_multas")
empresasDF.createOrReplaceTempView("v_empresas")
tipoInfraccionDF.createOrReplaceTempView("v_tipos")

println(">>> Vistas SQL registradas correctamente.")


// REQ 1: SELECCIÓN USANDO JOIN

println("\n--- 1. Selección usando JOIN (Multa + Nombre Empresa) ---")
spark.sql("""
    SELECT 
        m.codigo_papeleta, 
        e.nombre AS nombre_empresa, 
        m.monto_infraccion
    FROM v_multas m
    JOIN v_empresas e ON m.fk_empresa = e.id
    WHERE m.monto_infraccion > 2000
    LIMIT 5
""").show(truncate = false)

// REQ 2: DOS CONSULTAS USANDO GROUP BY Y COUNT

println("\n--- 2.1. GroupBy + Count: Total de multas por Tipo de Servicio ---")
spark.sql("""
    SELECT tipo_servicio, COUNT(*) as cantidad
    FROM v_multas
    GROUP BY tipo_servicio
    ORDER BY cantidad DESC
    LIMIT 5
""").show(false)

println("\n--- 2.2. GroupBy + Count: Total de multas por Ubigeo (Distrito) ---")
spark.sql("""
    SELECT fk_ubigeo, COUNT(*) as total_infracciones
    FROM v_multas
    GROUP BY fk_ubigeo
    ORDER BY total_infracciones DESC
    LIMIT 5
""").show()

// REQ 3: DOS CONSULTAS USANDO ORDER BY COMBINADO CON OTROS

println("\n--- 3.1. OrderBy + Where: Las 5 multas más caras de Transporte Público ---")
spark.sql("""
    SELECT placa_vehiculo, tipo_transporte, monto_infraccion
    FROM v_multas
    WHERE tipo_transporte = 'PUBLICO'
    ORDER BY monto_infraccion DESC
    LIMIT 5
""").show()

println("\n--- 3.2. OrderBy + Join: Infracciones ordenadas por descripción de falta ---")
spark.sql("""
    SELECT 
        t.descripcion, 
        m.monto_infraccion
    FROM v_multas m
    JOIN v_tipos t ON m.fk_tipo_infraccion = t.id
    ORDER BY t.descripcion ASC, m.monto_infraccion DESC
    LIMIT 5
""").show(truncate = false)
