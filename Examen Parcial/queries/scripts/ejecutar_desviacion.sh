#!/bin/bash
# scripts/ejecutar_desviacion.sh

echo "=== Ejecutando Consulta: Desviación Estándar del MONTO ==="

JAR_FILE="$HOME/concytec_proyecto/jars/desviacion.jar"
INPUT_PATH="/user/hadoop/concytec/datos/data_concytec.csv"
OUTPUT_PATH="/user/hadoop/concytec/resultados/desviacion"
LOG_FILE="$HOME/concytec_proyecto/logs/desviacion_$(date +%Y%m%d_%H%M%S).log"

if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: No se encuentra el archivo $JAR_FILE"
    exit 1
fi

echo "1. Limpiando directorio de salida..."
hdfs dfs -rm -r $OUTPUT_PATH 2>/dev/null

echo "2. Ejecutando job de MapReduce..."
hadoop jar $JAR_FILE \
    com.concytec.DesviacionDriver \
    $INPUT_PATH \
    $OUTPUT_PATH \
    2>&1 | tee $LOG_FILE

if [ $? -eq 0 ]; then
    echo "3. Job completado exitosamente"
    echo "4. Resultado:"
    hdfs dfs -cat $OUTPUT_PATH/part-r-00000
    hdfs dfs -get $OUTPUT_PATH/part-r-00000 ~/concytec_proyecto/resultado_desviacion.txt
    echo "✅ Desviación estándar calculada y guardada"
else
    echo "❌ ERROR: El job falló"
    exit 1
fi
