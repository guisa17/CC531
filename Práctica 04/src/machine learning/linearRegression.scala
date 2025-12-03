import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions._

println("--- REGRESION LINEAL V1 (PREDECIR MONTO) ---")

// 1. CARGA DE DATOS
val rawData = spark.read.option("header", "true")
                        .option("inferSchema", "true")
                        .option("delimiter", ",") 
                        .csv("data/infracciones_clean.csv")

// 2. LIMPIEZA PARA REGRESION
// Eliminamos filas donde el monto sea 0 o nulo y rellenamos vacios
val data = rawData.na.fill(0, Seq("HORA_INFRACCION"))
                  .na.fill("DESCONOCIDO", Seq("TIPO_TRANSPORTE", "TIPO_SERVICIO"))
                  .filter("MONTO_INFRACCION > 0")

// Split 70% Entrenamiento - 30% Prueba
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234)

// 3. CONFIGURACION DEL PIPELINE

// A. Indexar Variables Categoricas (Texto a Numero)
val transporteIndexer = new StringIndexer()
  .setInputCol("TIPO_TRANSPORTE")
  .setOutputCol("idx_transporte")
  .setHandleInvalid("keep")

val servicioIndexer = new StringIndexer()
  .setInputCol("TIPO_SERVICIO")
  .setOutputCol("idx_servicio")
  .setHandleInvalid("keep")

// B. Vector de Caracteristicas
// En la v1 solo usamos: Transporte, Servicio y Hora
val assembler = new VectorAssembler()
  .setInputCols(Array("idx_transporte", "idx_servicio", "HORA_INFRACCION"))
  .setOutputCol("features")

// C. Algoritmo Regresion Lineal
val lr = new LinearRegression()
  .setLabelCol("MONTO_INFRACCION")
  .setFeaturesCol("features")
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

// 4. EJECUCION
val pipeline = new Pipeline().setStages(Array(transporteIndexer, servicioIndexer, assembler, lr))

val model = pipeline.fit(trainingData)
val predictions = model.transform(testData)

// 5. EVALUACION DE METRICAS
val evaluatorRMSE = new RegressionEvaluator()
  .setLabelCol("MONTO_INFRACCION")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val evaluatorR2 = new RegressionEvaluator()
  .setLabelCol("MONTO_INFRACCION")
  .setPredictionCol("prediction")
  .setMetricName("r2")

val rmse = evaluatorRMSE.evaluate(predictions)
val r2 = evaluatorR2.evaluate(predictions)

// 6. MOSTRAR RESULTADOS (TABLA FINAL)
// Nota: Accuracy/Recall no aplican a regresion, usamos R2 y RMSE
val resultados = Seq(
  ("Linear Regression v1", f"$r2%.4f (R2)", "N/A", "N/A", f"$rmse%.4f (RMSE)")
).toDF("Modelo", "Accuracy (R2)", "F1-Score", "Recall", "Perdida (RMSE)")

println("\n--- TABLA DE RESULTADOS ---")
resultados.show(false)

// 7. EJEMPLOS DE PREDICCIONES (Requisito del PDF)
println("\n--- EJEMPLOS DE PREDICCIONES (Monto Real vs Predicho) ---")
predictions.select("TIPO_TRANSPORTE", "HORA_INFRACCION", "MONTO_INFRACCION", "prediction")
           .show(10, false)

// Interpretacion rapida en consola
println(f"Interpretacion: El modelo explica el ${(r2 * 100)}%.2f%% de la varianza del monto.")

System.exit(0)