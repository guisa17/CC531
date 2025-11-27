println("=" * 70)
println("CONSULTA 1: Analisis de Visitas por Sede, Sala y Genero")
println("=" * 70)

println("\nPregunta: ¿Cuales son las combinaciones de Sede + Sala + Genero con mas visitas?")
println("Campos utilizados: VISITA_SEDE, VISITA_SALA, CIUDADANO_SEXO")

// Cargar datos limpios
val datosConsulta1 = sc.objectFile[(String, String, String, String, String, String, 
                    String, String, String, String, String, String)]("output/datos_limpios_rdd")

println(s"\n[1] Datos cargados: ${datosConsulta1.count()} registros")

// MapReduce: Agrupar por (Sede, Sala, Genero) y contar
println("\n[2] Aplicando MapReduce...")

val resultadoFinal = datosConsulta1
  .map { registro =>
    val sede = registro._2    // VISITA_SEDE
    val sala = registro._3    // VISITA_SALA
    val genero = registro._9  // CIUDADANO_SEXO
    
    // Map: Crear clave compuesta (sede, sala, genero) con valor 1
    ((sede, sala, genero), 1)
  }
  .reduceByKey(_ + _)  // Reduce: Sumar visitas por cada combinacion
  .map { case ((sede, sala, genero), count) =>
    (count, sede, sala, genero)  // Invertir para ordenar por count
  }
  .sortBy(_._1, ascending = false)  // Ordenar descendente por cantidad

// Mostrar Top 20
println("\n[3] TOP 20 COMBINACIONES (Sede + Sala + Genero):")
println("-" * 70)
println(f"${"Visitas"}%-10s | ${"Sede"}%-30s | ${"Sala"}%-20s | ${"Genero"}%-6s")
println("-" * 70)

resultadoFinal.take(20).foreach { case (count, sede, sala, genero) =>
  val generoDisplay = if (genero.isEmpty) "N/A" else genero
  println(f"$count%,10d | ${sede}%-30s | ${sala}%-20s | ${generoDisplay}%-6s")
}

// Analisis adicional: Total por genero
println("\n[4] DISTRIBUCION GENERAL POR GENERO:")
println("-" * 40)

val porGenero = datosConsulta1
  .map(registro => (registro._9, 1))  // (genero, 1)
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)

val totalRegs = datosConsulta1.count()

porGenero.collect().foreach { case (genero, count) =>
  val generoDisplay = if (genero.isEmpty) "No especificado" else genero
  val porcentaje = (count.toDouble / totalRegs) * 100
  println(f"${generoDisplay}%-20s : $count%,10d visitas ($porcentaje%5.2f%%)")
}

// Guardar resultado
println("\n[5] Guardando resultado...")
val csvResultado = resultadoFinal.map { case (count, sede, sala, genero) =>
  s"$count,$sede,$sala,$genero"
}

val csvHeader = sc.parallelize(Seq("Visitas,Sede,Sala,Genero"))
csvHeader.union(csvResultado).coalesce(1).saveAsTextFile("output/consulta_1_sede_sala_genero")

println("- Resultado guardado en: output/consulta_1_sede_sala_genero")
println("\n" + "=" * 70)
println("CONSULTA 1 COMPLETADA")
println("=" * 70)
