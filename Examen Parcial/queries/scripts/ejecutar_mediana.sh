#!/bin/bash
# scripts/ejecutar_mediana.sh

echo "=== Ejecutando Consulta: Mediana del MONTO ==="

JAR_FILE="$HOME/concytec_proyecto/jars/mediana.jar"
INPUT_PATH="/user/hadoop/concytec/datos/data_concytec.csv"
OUTPUT_PATH="/user/hadoop/concytec/resultados/mediana"
LOG_FILE="$HOME/concytec_proyecto/logs/mediana_$(date +%Y%m%d_%H%M%S).log"

if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: No se encuentra el archivo $JAR_FILE"
    exit 1
fi

echo "1. Limpiando directorio de salida..."
hdfs dfs -rm -r $OUTPUT_PATH 2>/dev/null

echo "2. Ejecutando job de MapReduce..."
hadoop jar $JAR_FILE \
    com.concytec.MedianaDriver \
    $INPUT_PATH \
    $OUTPUT_PATH \
    2>&1 | tee $LOG_FILE

if [ $? -eq 0 ]; then
    echo "3. Job completado exitosamente"
    echo "4. Resultado:"
    hdfs dfs -cat $OUTPUT_PATH/part-r-00000
    hdfs dfs -get $OUTPUT_PATH/part-r-00000 ~/concytec_proyecto/resultado_mediana.txt
    echo "✅ Mediana calculada y guardada"
else
    echo "❌ ERROR: El job falló"
    exit 1
fi
