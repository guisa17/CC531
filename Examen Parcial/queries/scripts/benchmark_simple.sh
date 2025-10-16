#!/bin/bash
export LC_NUMERIC="C"
# =========================================================
# Benchmark Hadoop - Consultas Simples (1 MapReduce)
# =========================================================
# Uso:
#   ./benchmark_simple.sh <clase_principal> <nombre_prueba>
# Ejemplo:
#   ./benchmark_simple.sh Consulta2.MedianDriver Mediana
# =========================================================

if [ $# -lt 2 ]; then
  echo "Uso: $0 <clase_principal> <nombre_prueba>"
  echo "Ejemplo: $0 Consulta1.MeanDriver Promedio"
  exit 1
fi

MAIN_CLASS=$1
TEST_NAME=$2

# Configuración de rutas
JAR_FILE="../jars/consultas-jar.jar"
INPUT_DIR="/user/hadoop/concytec/datos"
OUTPUT_DIR="/user/hadoop/concytec/resultados/${TEST_NAME,,}"
LOG_FILE="benchmark_results_simple.csv"

# Crear cabecera si no existe
if [ ! -f "$LOG_FILE" ]; then
  echo "Fecha,Prueba,Clase,Duración(s)" > $LOG_FILE
fi

# Verificar que existe el JAR
if [ ! -f "$JAR_FILE" ]; then
  echo "ERROR: No se encuentra el archivo JAR: $JAR_FILE"
  exit 1
fi

# Eliminar salida previa si existe
echo "Verificando directorio de salida..."
hdfs dfs -test -d $OUTPUT_DIR
if [ $? -eq 0 ]; then
  echo "Eliminando $OUTPUT_DIR anterior..."
  hdfs dfs -rm -r -f $OUTPUT_DIR
fi

# Ejecución y cronometraje
echo "========================================="
echo "Ejecutando: $TEST_NAME"
echo "Clase: $MAIN_CLASS"
echo "JAR: $JAR_FILE"
echo "Input: $INPUT_DIR"
echo "Output: $OUTPUT_DIR"
echo "========================================="
START=$(date +%s.%N)

hadoop jar $JAR_FILE $MAIN_CLASS $INPUT_DIR $OUTPUT_DIR
JOB_STATUS=$?

END=$(date +%s.%N)
DURATION=$(echo "scale=2; $END - $START" | bc)

if [ $JOB_STATUS -eq 0 ]; then
  echo "========================================="
  echo "[OK] $TEST_NAME completado exitosamente"
  echo "Duración: ${DURATION}s"
  echo "========================================="
  echo "$(date '+%Y-%m-%d %H:%M:%S'),$TEST_NAME,$MAIN_CLASS,$DURATION" >> $LOG_FILE
  
  # Mostrar resultados
  echo ""
  echo "Resultados guardados en HDFS:"
  hdfs dfs -ls $OUTPUT_DIR
  echo ""
  echo "Primeras líneas del resultado:"
  hdfs dfs -cat $OUTPUT_DIR/part-* | head -n 20
else
  echo "========================================="
  echo "[ERROR] Error durante la ejecución de $TEST_NAME"
  echo "========================================="
  exit 1
fi

echo ""
echo "Duración total: ${DURATION}s"
echo "Resultados guardados en: $LOG_FILE"
echo ""
