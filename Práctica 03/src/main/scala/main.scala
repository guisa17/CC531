import org.apache.spark.sql.SparkSession

object TestSpark {
  def main(args: Array[String]): Unit = {
    // Configurar encoding para Windows
    System.setProperty("file.encoding", "UTF-8")
    
    val spark = SparkSession.builder()
      .appName("BNP Lab03")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    
    println("✅ Spark funcionando!")
    println(s"Versión: ${spark.version}")
    
    val datos = Seq(
      ("Lima", "Sala 1"),
      ("Cusco", "Sala 2")
    ).toDF("Sede", "Sala")
    
    datos.show()
    
    spark.stop()
  }
}