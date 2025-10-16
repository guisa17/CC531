#!/bin/bash
export LC_NUMERIC="C"
# =========================================================
# Benchmark Hadoop - Consultas Triples (3 MapReduce)
# =========================================================
# Uso:
#   ./benchmark_triple.sh <archivo_jar> <clase_principal> <nombre_prueba>
# Ejemplo:
#   ./benchmark_triple.sh PARCIALMACRO-1.0-SNAPSHOT.jar Consulta4.Consulta4Driver PromedioTipoEntidadSexo
# =========================================================

if [ $# -lt 3 ]; then
  echo "❌ Uso: $0 <archivo_jar> <clase_principal> <nombre_prueba>"
  exit 1
fi

JAR_FILE=$1
MAIN_CLASS=$2
TEST_NAME=$3

INPUT_DIR="/input_parcial"
OUTPUT1="/output_parcial1"
OUTPUT2="/output_parcial2"
OUTPUT3="/output_parcial3"

LOG_FILE="benchmark_results_triple.csv"

# Crear cabecera si no existe
if [ ! -f "$LOG_FILE" ]; then
  echo "Fecha,Prueba,Duración(s),UsoCPU(%),MemoriaPromedio(MB)" > $LOG_FILE
fi

# Eliminar salidas previas
for dir in $OUTPUT1 $OUTPUT2 $OUTPUT3; do
  hadoop fs -test -d $dir
  if [ $? -eq 0 ]; then
    echo "⚠️  Eliminando $dir anterior..."
    hadoop fs -rm -r -f $dir
  fi
done

# Monitoreo de CPU/memoria
MPSTAT_LOG="cpu_mem_${TEST_NAME}.log"
(
  echo "timestamp,cpu_idle,mem_used(MB),mem_free(MB)"
  while true; do
    timestamp=$(date '+%H:%M:%S')
    # Obtener uso de CPU y memoria en formato limpio
    cpu_idle=$(mpstat 1 1 | awk '/Average/ && $3 !~ /CPU/ {print $12}' | tr -d '[:space:]')
    if [ -z "$cpu_idle" ]; then cpu_idle=0; fi
    mem_line=$(free -m | awk '/Mem:/ {print $3","$4}')
    mem_used=$(echo "$mem_line" | cut -d',' -f1)
    mem_free=$(echo "$mem_line" | cut -d',' -f2)

# Registrar los valores correctamente delimitados
echo "$timestamp,$cpu_idle,$mem_used,$mem_free"

    sleep 2
  done
) > "$MPSTAT_LOG" &
MONITOR_PID=$!

# Ejecución y cronometraje
echo "Ejecutando $TEST_NAME..."
START=$(date +%s.%N)

hadoop jar $JAR_FILE $MAIN_CLASS $INPUT_DIR $OUTPUT1 $OUTPUT2 $OUTPUT3
JOB_STATUS=$?

END=$(date +%s.%N)
DURATION=$(echo "scale=2; $END - $START" | bc)

kill $MONITOR_PID >/dev/null 2>&1

# Cálculo de promedios
AVG_CPU_IDLE=$(awk -F',' 'NR>1 {sum+=$2;count++} END{if(count>0) print sum/count; else print 0}' $MPSTAT_LOG)
AVG_MEM_USED=$(awk -F',' 'NR>1 {sum+=$3;count++} END{if(count>0) print sum/count; else print 0}' $MPSTAT_LOG)
CPU_USAGE=$(echo "scale=2; 100 - $AVG_CPU_IDLE" | bc)

if [ $JOB_STATUS -eq 0 ]; then
  echo " $TEST_NAME completado en ${DURATION}s."
  echo "$(date '+%Y-%m-%d %H:%M:%S'),$TEST_NAME,$DURATION,$CPU_USAGE,$AVG_MEM_USED" >> $LOG_FILE
else
  echo "Error durante la ejecución de $TEST_NAME."
fi

echo " - Resultados guardados en $LOG_FILE"
echo " - CPU Promedio: $CPU_USAGE%"
echo " - Memoria Promedio: ${AVG_MEM_USED}MB"
echo " - Logs: $MPSTAT_LOG"

# Mostrar salida final
#echo "📄 Mostrando resultados de /output_parcial3:"
#hadoop fs -cat /output_parcial3/part-* | head -n 10
