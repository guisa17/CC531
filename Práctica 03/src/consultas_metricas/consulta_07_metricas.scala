println("=" * 70)
println("CONSULTA 7: Metricas Estadisticas de un Campo Numerico")
println("=" * 70)

println("\nPregunta: ¿Cuales son las metricas estadisticas (promedio, mediana, desviacion, max, min) de la edad de los visitantes?")
println("Campo utilizado: Edad (derivada de CIUDADANO_FECHA_NACIMIENTO)")

// Funcion para calcular edad desde fecha de nacimiento (formato YYYYMMDD)
def calcularEdad(fechaNacimiento: String): Int = {
    if (fechaNacimiento.isEmpty || fechaNacimiento.length != 8) return 0
    
    val anioNac = fechaNacimiento.substring(0, 4).toInt
    val mesNac = fechaNacimiento.substring(4, 6).toInt
    val diaNac = fechaNacimiento.substring(6, 8).toInt
    
    val anioActual = 2021
    val mesActual = 12
    val diaActual = 31
    
    var edad = anioActual - anioNac
    if (mesActual < mesNac || (mesActual == mesNac && diaActual < diaNac)) {
        edad -= 1
    }
    edad
}

// Cargar datos limpios
val datosConsulta7 = sc.objectFile[(String, String, String, String, String, String, 
                    String, String, String, String, String, String)]("output/datos_limpios_rdd")

println(s"\n[1] Datos cargados: ${datosConsulta7.count()} registros")

// Calcular edades validas
val edades = datosConsulta7
    .filter(registro => registro._8.length == 8 && registro._8.forall(_.isDigit))
    .map(registro => calcularEdad(registro._8))
    .filter(edad => edad > 0 && edad < 120)
    .cache()

val totalEdades = edades.count()
println(s"\n[2] Total de edades validas: $totalEdades")

// PROMEDIO (Mean)
val sumaEdades = edades.sum()
val promedio = sumaEdades / totalEdades

// MEDIANA (Median)
val edadesOrdenadas = edades.sortBy(x => x).collect()
val mediana = if (totalEdades % 2 == 0) {
    val mid = (totalEdades / 2).toInt
    (edadesOrdenadas(mid - 1) + edadesOrdenadas(mid)) / 2.0
} else {
    edadesOrdenadas((totalEdades / 2).toInt).toDouble
}

// DESVIACION ESTANDAR (Standard Deviation)
val varianza = edades.map(edad => math.pow(edad - promedio, 2)).sum() / totalEdades
val desviacionEstandar = math.sqrt(varianza)

// MAXIMO Y MINIMO
val edadMaxima = edades.max()
val edadMinima = edades.min()

println("\n[3] Resultados:")
println("=" * 70)
println(f"    Promedio:              ${promedio}%.2f años")
println(f"    Mediana:               ${mediana}%.2f años")
println(f"    Desviacion Estandar:   ${desviacionEstandar}%.2f")
println(f"    Mayor (Maximo):        ${edadMaxima}%d años")
println(f"    Menor (Minimo):        ${edadMinima}%d años")
println("=" * 70)

// Guardar resultados
val resultados = sc.parallelize(Seq(
    "=" * 70,
    "CONSULTA 7: Metricas Estadisticas de un Campo Numerico",
    "=" * 70,
    "",
    "Campo analizado: Edad (derivada de CIUDADANO_FECHA_NACIMIENTO)",
    s"Total de registros: $totalEdades",
    "",
    "METRICAS ESTADISTICAS:",
    "-" * 50,
    f"Promedio:              ${promedio}%.2f años",
    f"Mediana:               ${mediana}%.2f años",
    f"Desviacion Estandar:   ${desviacionEstandar}%.2f",
    f"Mayor (Maximo):        ${edadMaxima}%d años",
    f"Menor (Minimo):        ${edadMinima}%d años"
))

resultados.coalesce(1).saveAsTextFile("output/consulta_7_metricas_estadisticas")
println("\n[4] Resultados guardados en: output/consulta_7_metricas_estadisticas/")
println("=" * 70)
