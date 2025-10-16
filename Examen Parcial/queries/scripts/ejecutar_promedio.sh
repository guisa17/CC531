#!/bin/bash
# scripts/ejecutar_promedio.sh

echo "=== Ejecutando Consulta: Promedio del MONTO ==="

# Variables
JAR_FILE="$HOME/concytec_proyecto/jars/promedio.jar"
INPUT_PATH="/user/hadoop/concytec/datos/data_concytec.csv"
OUTPUT_PATH="/user/hadoop/concytec/resultados/promedio"
LOG_FILE="$HOME/concytec_proyecto/logs/promedio_$(date +%Y%m%d_%H%M%S).log"

# Verificar que el JAR existe
if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: No se encuentra el archivo $JAR_FILE"
    exit 1
fi

# Eliminar output anterior si existe
echo "1. Limpiando directorio de salida..."
hdfs dfs -rm -r $OUTPUT_PATH 2>/dev/null

# Ejecutar MapReduce
echo "2. Ejecutando job de MapReduce..."
hadoop jar $JAR_FILE \
    com.concytec.PromedioDriver \
    $INPUT_PATH \
    $OUTPUT_PATH \
    2>&1 | tee $LOG_FILE

# Verificar resultado
if [ $? -eq 0 ]; then
    echo "3. Job completado exitosamente"
    
    echo "4. Resultado:"
    hdfs dfs -cat $OUTPUT_PATH/part-r-00000
    
    echo "5. Guardando resultado localmente..."
    hdfs dfs -get $OUTPUT_PATH/part-r-00000 ~/concytec_proyecto/resultado_promedio.txt
    
    echo "✅ Promedio calculado y guardado en ~/concytec_proyecto/resultado_promedio.txt"
else
    echo "❌ ERROR: El job falló. Revisa el log en $LOG_FILE"
    exit 1
fi
