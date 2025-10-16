#!/bin/bash
# scripts/ejecutar_consulta1_mr3.sh

echo "=== Ejecutando Consulta 1: 3 MapReduce Encadenados ==="

JAR_FILE="$HOME/concytec_proyecto/jars/consulta1_mr3.jar"
INPUT_PATH="/user/hadoop/concytec/datos/data_concytec.csv"
OUTPUT_PATH="/user/hadoop/concytec/resultados/consulta1_mr3"
LOG_FILE="$HOME/concytec_proyecto/logs/consulta1_mr3_$(date +%Y%m%d_%H%M%S).log"

if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: No se encuentra el archivo $JAR_FILE"
    exit 1
fi

echo "1. Limpiando directorios de salida..."
hdfs dfs -rm -r ${OUTPUT_PATH}_temp1 2>/dev/null
hdfs dfs -rm -r ${OUTPUT_PATH}_temp2 2>/dev/null
hdfs dfs -rm -r ${OUTPUT_PATH}_final 2>/dev/null

echo "2. Ejecutando MapReduce 1..."
hadoop jar $JAR_FILE \
    com.concytec.Consulta1MR1Driver \
    $INPUT_PATH \
    ${OUTPUT_PATH}_temp1 \
    2>&1 | tee -a $LOG_FILE

echo "3. Ejecutando MapReduce 2..."
hadoop jar $JAR_FILE \
    com.concytec.Consulta1MR2Driver \
    ${OUTPUT_PATH}_temp1 \
    ${OUTPUT_PATH}_temp2 \
    2>&1 | tee -a $LOG_FILE

echo "4. Ejecutando MapReduce 3..."
hadoop jar $JAR_FILE \
    com.concytec.Consulta1MR3Driver \
    ${OUTPUT_PATH}_temp2 \
    ${OUTPUT_PATH}_final \
    2>&1 | tee -a $LOG_FILE

if [ $? -eq 0 ]; then
    echo "5. Jobs completados exitosamente"
    echo "6. Resultado final:"
    hdfs dfs -cat ${OUTPUT_PATH}_final/part-r-00000
    hdfs dfs -get ${OUTPUT_PATH}_final/part-r-00000 ~/concytec_proyecto/resultado_consulta1_mr3.txt
    echo "✅ Consulta 1 completada y guardada"
else
    echo "❌ ERROR: Algún job falló"
    exit 1
fi
