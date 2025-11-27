println("=" * 70)
println("CONSULTA 5: Buscar Texto en Campo y Agrupar por Categoria")
println("=" * 70)

println("\nPregunta: ¿Que sedes tienen mas visitas a la Hemeroteca?")
println("Campos utilizados: VISITA_SALA (busqueda de 'Hemeroteca'), VISITA_SEDE (agrupacion)")

// Cargar datos limpios
val datosConsulta5 = sc.objectFile[(String, String, String, String, String, String, 
                    String, String, String, String, String, String)]("output/datos_limpios_rdd")

println(s"\n[1] Datos cargados: ${datosConsulta5.count()} registros")

// Texto a buscar en la sala
val textoBusqueda = "Hemeroteca"

// Filtrar registros donde la sala contenga "Hemeroteca"
val datosFiltrados = datosConsulta5.filter { registro =>
    val sala = registro._3
    sala.contains(textoBusqueda)
}

val totalFiltrados = datosFiltrados.count()
println(s"\n[2] Registros de visitas a '$textoBusqueda': $totalFiltrados")

// Agrupar por sede y contar
val porSede = datosFiltrados
    .map(registro => (registro._2, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)

val resultadosSedes = porSede.collect()

println(s"\n[3] Distribucion de visitas a '$textoBusqueda' por Sede:")
println("-" * 70)
resultadosSedes.foreach { case (sede, total) =>
    val porcentaje = (total.toDouble / totalFiltrados) * 100
    println(f"    $sede%-50s : $total%,7d visitas (${porcentaje}%.2f%%)")
}

// Analisis adicional: Distribucion de genero en Hemeroteca
val porGenero = datosFiltrados
    .map(registro => (registro._9, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .collect()

println(s"\n[4] Distribucion por Genero en '$textoBusqueda':")
println("-" * 50)
porGenero.foreach { case (genero, total) =>
    val porcentaje = (total.toDouble / totalFiltrados) * 100
    val generoDesc = genero match {
        case "M" => "Masculino"
        case "F" => "Femenino"
        case _ => "No especificado"
    }
    println(f"    $generoDesc%-20s : $total%,7d visitas (${porcentaje}%.2f%%)")
}

// Comparacion: Hemeroteca vs Coleccion Peruana por sede
val coleccionPeruana = datosConsulta5.filter { registro =>
    val sala = registro._3
    sala.contains("Peruana")
}

val porSedeColeccion = coleccionPeruana
    .map(registro => (registro._2, 1))
    .reduceByKey(_ + _)
    .collect()
    .toMap

println(s"\n[5] Comparacion: Hemeroteca vs Coleccion Peruana por Sede:")
println("-" * 90)
println(f"    ${"Sede"}%-50s | ${"Hemeroteca"}%12s | ${"Col. Peruana"}%12s")
println("-" * 90)

resultadosSedes.foreach { case (sede, totalHeme) =>
    val totalCol = porSedeColeccion.getOrElse(sede, 0)
    println(f"    $sede%-50s | $totalHeme%,10d | $totalCol%,12d")
}

// Guardar resultados
val resultados = sc.parallelize(Seq(
    "=" * 70,
    "CONSULTA 5: Buscar Texto en Campo y Agrupar por Categoria",
    "=" * 70,
    "",
    s"Texto buscado en VISITA_SALA: '$textoBusqueda'",
    s"Campo de agrupacion: VISITA_SEDE",
    s"Total de visitas a '$textoBusqueda': $totalFiltrados",
    "",
    "DISTRIBUCION POR SEDE:",
    "-" * 70
) ++ resultadosSedes.map { case (sede, total) =>
    val porcentaje = (total.toDouble / totalFiltrados) * 100
    f"$sede%-50s : $total%,7d visitas (${porcentaje}%.2f%%)"
} ++ Seq(
    "",
    "DISTRIBUCION POR GENERO:",
    "-" * 50
) ++ porGenero.map { case (genero, total) =>
    val porcentaje = (total.toDouble / totalFiltrados) * 100
    val generoDesc = genero match {
        case "M" => "Masculino"
        case "F" => "Femenino"
        case _ => "No especificado"
    }
    f"$generoDesc%-20s : $total%,7d visitas (${porcentaje}%.2f%%)"
} ++ Seq(
    "",
    "COMPARACION HEMEROTECA VS COLECCION PERUANA:",
    "-" * 90,
    f"${"Sede"}%-50s | ${"Hemeroteca"}%12s | ${"Col. Peruana"}%12s",
    "-" * 90
) ++ resultadosSedes.map { case (sede, totalHeme) =>
    val totalCol = porSedeColeccion.getOrElse(sede, 0)
    f"$sede%-50s | $totalHeme%,10d | $totalCol%,12d"
})

resultados.coalesce(1).saveAsTextFile("output/consulta_5_busqueda_texto")
println("\n[6] Resultados guardados en: output/consulta_5_busqueda_texto/")
println("=" * 70)
