#!/bin/bash
export LC_NUMERIC="C"
# =========================================================
# Benchmark Hadoop - Consultas Encadenadas (3 MapReduce)
# =========================================================
# Uso:
#   ./benchmark_triple.sh <clase_principal> <nombre_prueba>
# Ejemplo:
#   ./benchmark_triple.sh Consulta4.Consulta4Driver Consulta4_Encadenada
# =========================================================

if [ $# -lt 2 ]; then
  echo "Uso: $0 <clase_principal> <nombre_prueba>"
  echo "Ejemplo: $0 Consulta4.Consulta4Driver Consulta4"
  exit 1
fi

MAIN_CLASS=$1
TEST_NAME=$2

# Configuración de rutas
JAR_FILE="../jars/consultas-jar.jar"
INPUT_DIR="/user/hadoop/concytec/datos"
OUTPUT_DIR="/user/hadoop/concytec/resultados/${TEST_NAME,,}"
LOG_FILE="benchmark_results_triple.csv"

# Crear cabecera si no existe
if [ ! -f "$LOG_FILE" ]; then
  echo "Fecha,Prueba,Clase,Duración_Total(s),MR1(s),MR2(s),MR3(s)" > $LOG_FILE
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
echo "Ejecutando: $TEST_NAME (Consulta Encadenada)"
echo "Clase: $MAIN_CLASS"
echo "JAR: $JAR_FILE"
echo "Input: $INPUT_DIR"
echo "Output: $OUTPUT_DIR"
echo "========================================="
echo ""
echo "Esta consulta ejecutará 3 MapReduce encadenados..."
echo ""

START=$(date +%s.%N)

hadoop jar $JAR_FILE $MAIN_CLASS $INPUT_DIR $OUTPUT_DIR
JOB_STATUS=$?

END=$(date +%s.%N)
DURATION=$(echo "scale=2; $END - $START" | bc)

if [ $JOB_STATUS -eq 0 ]; then
  echo ""
  echo "========================================="
  echo "[OK] $TEST_NAME completado exitosamente"
  echo "Duración total: ${DURATION}s"
  echo "========================================="
  echo "$(date '+%Y-%m-%d %H:%M:%S'),$TEST_NAME,$MAIN_CLASS,$DURATION,N/A,N/A,N/A" >> $LOG_FILE
  
  # Mostrar resultados finales
  echo ""
  echo "Resultados guardados en HDFS:"
  hdfs dfs -ls -R $OUTPUT_DIR
  echo ""
  echo "Resultado final (después de los 3 MR):"
  hdfs dfs -cat $OUTPUT_DIR/part-* 2>/dev/null | head -n 20
  
  # Si hay directorios intermedios, mostrarlos
  echo ""
  echo "Directorios intermedios:"
  hdfs dfs -ls $OUTPUT_DIR/
else
  echo ""
  echo "========================================="
  echo "[ERROR] Error durante la ejecución de $TEST_NAME"
  echo "========================================="
  exit 1
fi

echo ""
echo "Duración total de los 3 MapReduce: ${DURATION}s"
echo "Resultados guardados en: $LOG_FILE"
echo ""