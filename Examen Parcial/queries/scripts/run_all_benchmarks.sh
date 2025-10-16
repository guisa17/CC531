#!/bin/bash
# =========================================================
# Script Maestro - Ejecutar Todos los Benchmarks
# =========================================================
# Este script ejecuta todas las consultas del proyecto CONCYTEC
# =========================================================

echo "=================================================="
echo "  BENCHMARK COMPLETO - PROYECTO CONCYTEC"
echo "=================================================="
echo ""

# Directorio de scripts
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# =========================================================
# PARTE 1: Consultas Simples (Estadísticas Básicas)
# =========================================================
echo "PARTE 1: ESTADISTICAS BASICAS (1 MapReduce cada una)"
echo "=================================================="
echo ""

echo "[1/7] Ejecutando Consulta 1: Promedio (Mean)..."
./benchmark_simple.sh Consulta1.MeanDriver Promedio
echo ""

echo "[2/7] Ejecutando Consulta 2: Mediana (Median)..."
./benchmark_simple.sh Consulta2.MedianDriver Mediana
echo ""

echo "[3/7] Ejecutando Consulta 3: Desviación Estándar (Stdev)..."
./benchmark_simple.sh Consulta3.StdevDriver Desviacion
echo ""

# =========================================================
# PARTE 2: Consultas Encadenadas (3 MapReduce)
# =========================================================
echo "=================================================="
echo "PARTE 2: CONSULTAS ENCADENADAS (3 MapReduce)"
echo "=================================================="
echo ""

echo "[4/7] Ejecutando Consulta 4 (3 MapReduce encadenados)..."
./benchmark_triple.sh Consulta4.Consulta4Driver Consulta4
echo ""

echo "[5/7] Ejecutando Consulta 5 (3 MapReduce encadenados)..."
./benchmark_triple.sh Consulta5.Consulta5Driver Consulta5
echo ""

# =========================================================
# PARTE 3: Modelo de Machine Learning
# =========================================================
echo "=================================================="
echo "PARTE 3: MODELO DE MACHINE LEARNING"
echo "=================================================="
echo ""

echo "[6/7] Ejecutando Consulta 6 (3 MapReduce encadenados)..."
./benchmark_triple.sh Consulta6.RegressionDriver Consulta6
echo ""

echo "[7/7] Ejecutando Consulta 7: Clasificación..."
./benchmark_simple.sh Consulta7.ClassificationDriver Clasificacion
echo ""

# =========================================================
# Resumen Final
# =========================================================
echo "=================================================="
echo "[OK] TODAS LAS CONSULTAS COMPLETADAS"
echo "=================================================="
echo ""
echo "Resumen de resultados:"
echo ""

if [ -f "benchmark_results_simple.csv" ]; then
  echo "Consultas Simples:"
  cat benchmark_results_simple.csv
  echo ""
fi

if [ -f "benchmark_results_triple.csv" ]; then
  echo "Consultas Encadenadas:"
  cat benchmark_results_triple.csv
  echo ""
fi

echo "=================================================="
echo "Resultados guardados en HDFS:"
hdfs dfs -ls /user/hadoop/concytec/resultados/
echo ""

echo "=================================================="
echo "Benchmark completado exitosamente"
echo "=================================================="
