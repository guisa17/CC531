import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions._

println("--- RANDOM FOREST (CLASIFICACION) ---")

// 1. CARGA DE DATOS
val rawData = spark.read.option("header", "true")
                        .option("inferSchema", "true") 
                        .option("delimiter", ",") 
                        .csv("data/infracciones_clean.csv")

// 2. LIMPIEZA BASICA
val data = rawData.na.fill(0, Seq("MONTO_INFRACCION", "HORA_INFRACCION"))
                  .na.fill("DESCONOCIDO", Seq("TIPO_TRANSPORTE", "TIPO_SERVICIO", "SE_PAGO_MULTAS"))

// Split 70% Entrenamiento - 30% Prueba
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234)

// 3. CONFIGURACION DEL PIPELINE

// A. Indexar Etiqueta (Target)
val labelIndexer = new StringIndexer()
  .setInputCol("SE_PAGO_MULTAS")
  .setOutputCol("indexedLabel")
  .fit(data)

// B. Indexar Variables Categoricas
val transporteIndexer = new StringIndexer()
  .setInputCol("TIPO_TRANSPORTE")
  .setOutputCol("idx_transporte")
  .setHandleInvalid("keep")

val servicioIndexer = new StringIndexer()
  .setInputCol("TIPO_SERVICIO")
  .setOutputCol("idx_servicio")
  .setHandleInvalid("keep")

// C. Vector de Caracteristicas
val assembler = new VectorAssembler()
  .setInputCols(Array("idx_transporte", "idx_servicio", "MONTO_INFRACCION", "HORA_INFRACCION"))
  .setOutputCol("indexedFeatures")

// D. Algoritmo Random Forest
val rf = new RandomForestClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures")
  .setNumTrees(20)

// E. Decodificar Prediccion (Numero a Texto)
val labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels)

// 4. EJECUCION
val pipeline = new Pipeline().setStages(Array(labelIndexer, transporteIndexer, servicioIndexer, assembler, rf, labelConverter))

val model = pipeline.fit(trainingData)
val predictions = model.transform(testData)

// 5. EVALUACION DE METRICAS
val evaluatorAcc = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
val evaluatorF1 = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("f1")
val evaluatorRec = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("weightedRecall")

val accuracy = evaluatorAcc.evaluate(predictions)
val f1 = evaluatorF1.evaluate(predictions)
val recall = evaluatorRec.evaluate(predictions)
val loss = 1.0 - accuracy

// 6. MOSTRAR RESULTADOS (TABLA FINAL)
val resultados = Seq(
  ("Random Forest", f"$accuracy%.4f", f"$f1%.4f", f"$recall%.4f", f"$loss%.4f")
).toDF("Modelo", "Accuracy", "F1-Score", "Recall", "Perdida")

println("\n--- TABLA DE RESULTADOS ---")
resultados.show(false)

// 7. EJEMPLOS DE PREDICCIONES (Requisito del PDF)
println("\n--- EJEMPLOS DE PREDICCIONES (Real vs Predicho) ---")
predictions.select("TIPO_TRANSPORTE", "MONTO_INFRACCION", "SE_PAGO_MULTAS", "predictedLabel", "probability")
           .show(10, false)

// Interpretacion rapida en consola
println(f"Interpretacion: El modelo acierta el estado de pago el ${(accuracy * 100)}%.2f%% de las veces.")

System.exit(0)