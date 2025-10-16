#!/bin/bash
# scripts/ejecutar_todas_consultas.sh

echo "=========================================="
echo "  EJECUTANDO TODAS LAS CONSULTAS"
echo "=========================================="

SCRIPTS_DIR="$HOME/concytec_proyecto/scripts"

# Consulta 1: Promedio
echo -e "\n[1/7] Ejecutando Promedio..."
bash $SCRIPTS_DIR/ejecutar_promedio.sh
sleep 3

# Consulta 2: Mediana
echo -e "\n[2/7] Ejecutando Mediana..."
bash $SCRIPTS_DIR/ejecutar_mediana.sh
sleep 3

# Consulta 3: Desviación Estándar
echo -e "\n[3/7] Ejecutando Desviación Estándar..."
bash $SCRIPTS_DIR/ejecutar_desviacion.sh
sleep 3

# Consulta 4: Primera consulta con 3 MR
echo -e "\n[4/7] Ejecutando Consulta 1 (3 MapReduce)..."
bash $SCRIPTS_DIR/ejecutar_consulta1_mr3.sh
sleep 3

# Consulta 5: Segunda consulta con 3 MR
echo -e "\n[5/7] Ejecutando Consulta 2 (3 MapReduce)..."
bash $SCRIPTS_DIR/ejecutar_consulta2_mr3.sh
sleep 3

# Consulta 6: Regresión
echo -e "\n[6/7] Ejecutando Modelo de Regresión..."
bash $SCRIPTS_DIR/ejecutar_regresion.sh
sleep 3

# Consulta 7: Clasificación
echo -e "\n[7/7] Ejecutando Modelo de Clasificación..."
bash $SCRIPTS_DIR/ejecutar_clasificacion.sh

echo -e "\n=========================================="
echo "  TODAS LAS CONSULTAS COMPLETADAS"
echo "=========================================="
echo ""
echo "Resultados guardados en: ~/concytec_proyecto/"
ls -lh ~/concytec_proyecto/resultado_*.txt
