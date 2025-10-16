#!/bin/bash
# =========================================================
# Script de Preparación del Entorno
# =========================================================
# Este script prepara el entorno HDFS antes de ejecutar las consultas
# =========================================================

echo "=================================================="
echo "  PREPARACION DEL ENTORNO - PROYECTO CONCYTEC"
echo "=================================================="
echo ""

# Verificar que Hadoop está corriendo
echo "Verificando que Hadoop está activo..."
jps | grep -q "NameNode"
if [ $? -ne 0 ]; then
    echo "[ERROR] NameNode no está corriendo"
    echo "   Ejecuta: start-dfs.sh && start-yarn.sh"
    exit 1
fi
echo "[OK] Hadoop está activo"
echo ""

# Crear estructura de directorios en HDFS
echo "Creando estructura de directorios en HDFS..."
hdfs dfs -mkdir -p /user/hadoop/concytec/datos
hdfs dfs -mkdir -p /user/hadoop/concytec/resultados
echo "[OK] Directorios creados"
echo ""

# Verificar que existe el archivo de datos
echo "Verificando archivo de datos..."
hdfs dfs -test -f /user/hadoop/concytec/datos/data_concytec.csv
if [ $? -ne 0 ]; then
    echo "[ADVERTENCIA] No se encuentra data_concytec.csv en HDFS"
    echo ""
    echo "   Para subir el archivo, ejecuta:"
    echo "   hdfs dfs -put ~/data_concytec.csv /user/hadoop/concytec/datos/"
    echo ""
    
    # Verificar si existe en el home local
    if [ -f ~/data_concytec.csv ]; then
        echo "   Se encontró el archivo en ~/data_concytec.csv"
        read -p "   Deseas subirlo ahora? (s/n): " respuesta
        if [ "$respuesta" = "s" ] || [ "$respuesta" = "S" ]; then
            echo "   Subiendo archivo..."
            hdfs dfs -put ~/data_concytec.csv /user/hadoop/concytec/datos/
            echo "   [OK] Archivo subido correctamente"
        fi
    else
        echo "   [ERROR] Archivo no encontrado en ~/data_concytec.csv"
        echo "   Debes subir el archivo primero con WinSCP o Termius"
        exit 1
    fi
else
    echo "[OK] Archivo de datos encontrado en HDFS"
fi
echo ""

# Mostrar información del archivo
echo "Información del archivo:"
hdfs dfs -stat "Nombre: %n | Tamaño: %b bytes | Modificado: %y" /user/hadoop/concytec/datos/data_concytec.csv
echo ""

# Contar registros
echo "Contando registros (puede tomar un momento)..."
TOTAL_LINES=$(hdfs dfs -cat /user/hadoop/concytec/datos/data_concytec.csv | wc -l)
TOTAL_RECORDS=$((TOTAL_LINES - 1))  # Restar el header
echo "   Total de líneas: $TOTAL_LINES"
echo "   Total de registros (sin header): $TOTAL_RECORDS"
echo ""

# Mostrar muestra de datos
echo "Muestra de datos (primeras 5 líneas):"
hdfs dfs -cat /user/hadoop/concytec/datos/data_concytec.csv | head -n 5
echo ""

# Verificar que existe el JAR
echo "Verificando archivo JAR..."
JAR_FILE="$(dirname "$0")/../jars/consultas-jar.jar"
if [ -f "$JAR_FILE" ]; then
    echo "[OK] JAR encontrado: $JAR_FILE"
    echo "   Tamaño: $(ls -lh "$JAR_FILE" | awk '{print $5}')"
else
    echo "[ERROR] No se encuentra el archivo JAR en $JAR_FILE"
    echo "   Debes compilar y colocar el JAR en la carpeta jars/"
    exit 1
fi
echo ""

# Limpiar resultados anteriores (opcional)
echo "Deseas limpiar resultados anteriores en HDFS? (s/n)"
read -p "   Respuesta: " limpiar
if [ "$limpiar" = "s" ] || [ "$limpiar" = "S" ]; then
    echo "   Limpiando directorios de resultados..."
    hdfs dfs -rm -r -f /user/hadoop/concytec/resultados/*
    echo "   [OK] Resultados anteriores eliminados"
fi
echo ""

# Mostrar estructura final
echo "Estructura actual en HDFS:"
hdfs dfs -ls -R /user/hadoop/concytec/
echo ""

echo "=================================================="
echo "[OK] Entorno preparado correctamente"
echo "=================================================="
echo ""
echo "Ahora puedes ejecutar los benchmarks:"
echo "   ./run_all_benchmarks.sh        - Ejecutar todas las consultas"
echo "   ./benchmark_simple.sh <clase> <nombre>  - Ejecutar una consulta simple"
echo "   ./benchmark_triple.sh <clase> <nombre>  - Ejecutar una consulta encadenada"
echo ""
