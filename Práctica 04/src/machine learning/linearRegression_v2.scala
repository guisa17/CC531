import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions._

println("--- REGRESION LINEAL V2 (INTERNAMIENTO + LICENCIA) ---")

// 1. CARGA DE DATOS
val rawData = spark.read.option("header", "true")
                        .option("inferSchema", "true")
                        .option("delimiter", ",") 
                        .csv("data/infracciones_clean.csv")

// 2. LIMPIEZA
// Rellenamos nulos en las nuevas columnas clave
val data = rawData.na.fill(0, Seq("HORA_INFRACCION"))
                  .na.fill("DESCONOCIDO", Seq("TIPO_TRANSPORTE", "TIPO_SERVICIO", "INTERNAMIENTO_VEHICULO", "CONDUCTOR_CATEGORIA_LICENCIA"))
                  .filter("MONTO_INFRACCION > 0")

// Split 70% Entrenamiento - 30% Prueba
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234)

// 3. CONFIGURACION DEL PIPELINE

// A. Indexadores (Uno para cada columna de texto)
val indexerT = new StringIndexer().setInputCol("TIPO_TRANSPORTE").setOutputCol("idx_transporte").setHandleInvalid("keep")
val indexerS = new StringIndexer().setInputCol("TIPO_SERVICIO").setOutputCol("idx_servicio").setHandleInvalid("keep")
// Nuevas variables
val indexerI = new StringIndexer().setInputCol("INTERNAMIENTO_VEHICULO").setOutputCol("idx_internamiento").setHandleInvalid("keep")
val indexerL = new StringIndexer().setInputCol("CONDUCTOR_CATEGORIA_LICENCIA").setOutputCol("idx_licencia").setHandleInvalid("keep")

// B. Vector Assembler
val assembler = new VectorAssembler()
  .setInputCols(Array("idx_transporte", "idx_servicio", "idx_internamiento", "idx_licencia", "HORA_INFRACCION"))
  .setOutputCol("features")

// C. Modelo
val lr = new LinearRegression()
  .setLabelCol("MONTO_INFRACCION")
  .setFeaturesCol("features")
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

// 4. EJECUCION
// Metemos todo al Pipeline
val pipeline = new Pipeline().setStages(Array(indexerT, indexerS, indexerI, indexerL, assembler, lr))

val model = pipeline.fit(trainingData)
val predictions = model.transform(testData)

// 5. EVALUACION (RMSE y R2 en Test)
val evaluatorRMSE = new RegressionEvaluator().setLabelCol("MONTO_INFRACCION").setPredictionCol("prediction").setMetricName("rmse")
val evaluatorR2 = new RegressionEvaluator().setLabelCol("MONTO_INFRACCION").setPredictionCol("prediction").setMetricName("r2")

val rmse = evaluatorRMSE.evaluate(predictions)
val r2 = evaluatorR2.evaluate(predictions)

// 6. RESULTADOS
val resultados = Seq(
  ("Linear Regression v2", f"$r2%.4f (R2)", "N/A", "N/A", f"$rmse%.4f (RMSE)")
).toDF("Modelo", "Accuracy (R2)", "F1-Score", "Recall", "Perdida (RMSE)")

println("\n--- TABLA DE RESULTADOS ---")
resultados.show(false)

// 7. EJEMPLOS DE PREDICCION
println("\n--- EJEMPLOS DE PREDICCIONES (Monto Real vs Predicho) ---")
predictions.select("INTERNAMIENTO_VEHICULO", "TIPO_TRANSPORTE", "MONTO_INFRACCION", "prediction")
           .show(10, false)

println(f"Interpretacion: Al agregar Internamiento y Licencia, el modelo explica el ${(r2 * 100)}%.2f%% de la varianza.")

System.exit(0)