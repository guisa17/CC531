import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions._

println("--- REGRESION LINEAL V3 (MODELO COMPLETO) ---")

// 1. CARGA
val rawData = spark.read.option("header", "true")
                        .option("inferSchema", "true")
                        .option("delimiter", ",") 
                        .csv("data/infracciones_clean.csv")

// 2. LIMPIEZA
// Lista de columnas categóricas
val columnasTexto = Array(
  "TIPO_TRANSPORTE", 
  "TIPO_SERVICIO", 
  "INTERNAMIENTO_VEHICULO", 
  "CONDUCTOR_CATEGORIA_LICENCIA", 
  "CODIGO_INFRACCION", 
  "TUC_ESTADO", 
  "CONDUCTOR_TENIA_LICENCIA"
)

// Rellenamos nulos
val data = rawData.na.fill(0, Seq("HORA_INFRACCION"))
                  .na.fill("DESCONOCIDO", columnasTexto)
                  .filter("MONTO_INFRACCION > 0")

val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234)

// 3. CONFIGURACION DINAMICA DEL PIPELINE

// A. Generar Indexadores automáticamente con un map
// Esto crea un array de StringIndexers, uno por cada columna de texto
val indexers = columnasTexto.map { colName =>
  new StringIndexer()
    .setInputCol(colName)
    .setOutputCol(s"idx_$colName")
    .setHandleInvalid("keep")
}

// B. Vector Assembler
// Juntamos los nombres de las columnas indexadas + la hora
val inputColsAssembler = columnasTexto.map(c => s"idx_$c") :+ "HORA_INFRACCION"

val assembler = new VectorAssembler()
  .setInputCols(inputColsAssembler)
  .setOutputCol("features")

// C. Modelo
val lr = new LinearRegression()
  .setLabelCol("MONTO_INFRACCION")
  .setFeaturesCol("features")
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

// 4. EJECUCION
// Unimos: (Array de Indexadores) + (Assembler) + (Modelo)
val stages = indexers.asInstanceOf[Array[PipelineStage]] ++ Array(assembler, lr)
val pipeline = new Pipeline().setStages(stages)

val model = pipeline.fit(trainingData)
val predictions = model.transform(testData)

// 5. EVALUACION
val evaluatorRMSE = new RegressionEvaluator().setLabelCol("MONTO_INFRACCION").setPredictionCol("prediction").setMetricName("rmse")
val evaluatorR2 = new RegressionEvaluator().setLabelCol("MONTO_INFRACCION").setPredictionCol("prediction").setMetricName("r2")

val rmse = evaluatorRMSE.evaluate(predictions)
val r2 = evaluatorR2.evaluate(predictions)

// 6. RESULTADOS
val resultados = Seq(
  ("Linear Regression v3", f"$r2%.4f (R2)", "N/A", "N/A", f"$rmse%.4f (RMSE)")
).toDF("Modelo", "Accuracy (R2)", "F1-Score", "Recall", "Perdida (RMSE)")

println("\n--- TABLA DE RESULTADOS (DEFINITIVA) ---")
resultados.show(false)

// 7. EJEMPLOS DE PREDICCION
println("\n--- EJEMPLOS DE PREDICCIONES (Monto Real vs Predicho) ---")
predictions.select("CODIGO_INFRACCION", "INTERNAMIENTO_VEHICULO", "MONTO_INFRACCION", "prediction")
           .show(10, false)

println(f"Interpretacion: Modelo final con R2 de ${(r2 * 100)}%.2f%%.")

System.exit(0)