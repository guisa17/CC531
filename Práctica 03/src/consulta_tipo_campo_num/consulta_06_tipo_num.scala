println("=" * 70)
println("CONSULTA 6: Agrupar por Tipo y Encontrar Mayores/Menores de Campo Numerico")
println("=" * 70)

println("\nPregunta: ¿Cual es la edad maxima y minima de visitantes por tipo de documento?")
println("Campos utilizados: CIUDADANO_TIPO_DOCUMENTO (agrupacion), edad derivada de CIUDADANO_FECHA_NACIMIENTO (numerico)")

// Funcion para calcular edad desde fecha de nacimiento (formato YYYYMMDD)
def calcularEdad(fechaNacimiento: String): Int = {
    if (fechaNacimiento.isEmpty || fechaNacimiento.length != 8) return 0
    
    val anioNac = fechaNacimiento.substring(0, 4).toInt
    val mesNac = fechaNacimiento.substring(4, 6).toInt
    val diaNac = fechaNacimiento.substring(6, 8).toInt
    
    val anioActual = 2021  // Asumimos año de corte del dataset
    val mesActual = 12
    val diaActual = 31
    
    var edad = anioActual - anioNac
    if (mesActual < mesNac || (mesActual == mesNac && diaActual < diaNac)) {
        edad -= 1
    }
    edad
}

// Cargar datos limpios
val datosConsulta6 = sc.objectFile[(String, String, String, String, String, String, 
                    String, String, String, String, String, String)]("output/datos_limpios_rdd")

println(s"\n[1] Datos cargados: ${datosConsulta6.count()} registros")

// Filtrar registros con fecha de nacimiento valida y calcular edad
val datosConEdad = datosConsulta6
    .filter(registro => registro._8.length == 8 && registro._8.forall(_.isDigit))
    .map { registro =>
        val tipoDoc = registro._6
        val edad = calcularEdad(registro._8)
        (tipoDoc, edad)
    }
    .filter { case (tipoDoc, edad) => edad > 0 && edad < 120 }  // Filtrar edades validas

val totalConEdad = datosConEdad.count()
println(s"\n[2] Registros con edad calculada: $totalConEdad")

// Agrupar por tipo de documento y encontrar max/min
val estadisticasPorTipo = datosConEdad
    .aggregateByKey((Int.MaxValue, Int.MinValue, 0L, 0L))(
        (acc, edad) => (
            math.min(acc._1, edad),     // Edad minima
            math.max(acc._2, edad),     // Edad maxima
            acc._3 + edad,              // Suma de edades
            acc._4 + 1                  // Contador
        ),
        (acc1, acc2) => (
            math.min(acc1._1, acc2._1),
            math.max(acc1._2, acc2._2),
            acc1._3 + acc2._3,
            acc1._4 + acc2._4
        )
    )
    .mapValues { case (minEdad, maxEdad, sumaEdades, count) =>
        val promedio = sumaEdades.toDouble / count
        (minEdad, maxEdad, promedio, count)
    }
    .sortBy(_._2._4, ascending = false)  // Ordenar por cantidad de visitantes

val resultados = estadisticasPorTipo.collect()

println(s"\n[3] Estadisticas de Edad por Tipo de Documento:")
println("-" * 90)
println(f"    ${"Tipo Documento"}%-15s | ${"Edad Min"}%8s | ${"Edad Max"}%8s | ${"Promedio"}%9s | ${"Total"}%10s")
println("-" * 90)
resultados.foreach { case (tipoDoc, (minEdad, maxEdad, promedio, count)) =>
    println(f"    $tipoDoc%-15s | $minEdad%8d | $maxEdad%8d | ${promedio}%9.2f | $count%,10d")
}

// Analisis adicional: Distribucion de edades por rangos
val rangosPorTipo = datosConEdad
    .map { case (tipoDoc, edad) =>
        val rango = edad match {
            case e if e < 18 => "Menor de 18"
            case e if e < 30 => "18-29"
            case e if e < 45 => "30-44"
            case e if e < 60 => "45-59"
            case _ => "60 o mas"
        }
        ((tipoDoc, rango), 1)
    }
    .reduceByKey(_ + _)
    .map { case ((tipoDoc, rango), count) => (tipoDoc, (rango, count)) }
    .groupByKey()
    .mapValues(_.toList.sortBy(_._2).reverse)
    .sortByKey()

println(s"\n[4] Distribucion por Rangos de Edad (Top Tipo de Documento):")
println("-" * 70)
rangosPorTipo.take(3).foreach { case (tipoDoc, rangos) =>
    println(s"    Tipo: $tipoDoc")
    rangos.foreach { case (rango, count) =>
        println(f"        $rango%-15s : $count%,7d visitantes")
    }
    println()
}

// Encontrar casos extremos: visitante mas joven y mas viejo
val visitanteMasJoven = datosConEdad.reduce((a, b) => if (a._2 < b._2) a else b)
val visitanteMasViejo = datosConEdad.reduce((a, b) => if (a._2 > b._2) a else b)

println(s"\n[5] Casos Extremos:")
println("-" * 50)
println(s"    Visitante mas joven: ${visitanteMasJoven._2} años (${visitanteMasJoven._1})")
println(s"    Visitante mas viejo: ${visitanteMasViejo._2} años (${visitanteMasViejo._1})")

// Guardar resultados
val resultadosGuardar = sc.parallelize(Seq(
    "=" * 70,
    "CONSULTA 6: Agrupar por Tipo y Encontrar Mayores/Menores",
    "=" * 70,
    "",
    "Campo de agrupacion: CIUDADANO_TIPO_DOCUMENTO",
    "Campo numerico: Edad (derivada de CIUDADANO_FECHA_NACIMIENTO)",
    s"Total de registros con edad: $totalConEdad",
    "",
    "ESTADISTICAS DE EDAD POR TIPO DE DOCUMENTO:",
    "-" * 90,
    f"${"Tipo Documento"}%-15s | ${"Edad Min"}%8s | ${"Edad Max"}%8s | ${"Promedio"}%9s | ${"Total"}%10s",
    "-" * 90
) ++ resultados.map { case (tipoDoc, (minEdad, maxEdad, promedio, count)) =>
    f"$tipoDoc%-15s | $minEdad%8d | $maxEdad%8d | ${promedio}%9.2f | $count%,10d"
} ++ Seq(
    "",
    "CASOS EXTREMOS:",
    "-" * 50,
    s"Visitante mas joven: ${visitanteMasJoven._2} años (${visitanteMasJoven._1})",
    s"Visitante mas viejo: ${visitanteMasViejo._2} años (${visitanteMasViejo._1})"
))

resultadosGuardar.coalesce(1).saveAsTextFile("output/consulta_6_tipo_numerico")
println("\n[6] Resultados guardados en: output/consulta_6_tipo_numerico/")
println("=" * 70)
