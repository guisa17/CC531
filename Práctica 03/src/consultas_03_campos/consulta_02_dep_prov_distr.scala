println("=" * 70)
println("CONSULTA 2: Analisis Demografico por Ubicacion Geografica")
println("=" * 70)

println("\nPregunta: ¿Que distritos de que provincias y departamentos tienen mas visitantes?")
println("Campos utilizados: CIUDADANO_DEPARTAMENTO, CIUDADANO_PROVINCIA, CIUDADANO_DISTRITO")

// Cargar datos limpios
type RegistroBNP = (String, String, String, String, String, String, 
                    String, String, String, String, String, String)

val datosParseados = sc.objectFile[RegistroBNP]("output/datos_limpios_rdd")

println(s"\n[1] Datos cargados: ${datosParseados.count()} registros")

// MapReduce: Agrupar por (Departamento, Provincia, Distrito) y contar
println("\n[2] Aplicando MapReduce...")

val visitasPorUbicacion = datosParseados
  .map { registro =>
    val departamento = registro._10  // CIUDADANO_DEPARTAMENTO
    val provincia = registro._11     // CIUDADANO_PROVINCIA
    val distrito = registro._12      // CIUDADANO_DISTRITO
    
    // Map: Crear clave compuesta (departamento, provincia, distrito) con valor 1
    ((departamento, provincia, distrito), 1)
  }
  .reduceByKey(_ + _)  // Reduce: Sumar visitas por ubicacion
  .map { case ((dpto, prov, dist), count) =>
    (count, dpto, prov, dist)  // Invertir para ordenar
  }
  .sortBy(_._1, ascending = false)  // Ordenar descendente

// Mostrar Top 30
println("\n[3] TOP 30 UBICACIONES (Departamento + Provincia + Distrito):")
println("-" * 80)
println(f"${"Visitas"}%-10s | ${"Departamento"}%-20s | ${"Provincia"}%-20s | ${"Distrito"}%-20s")
println("-" * 80)

visitasPorUbicacion.take(30).foreach { case (count, dpto, prov, dist) =>
  val dptoDisplay = if (dpto.isEmpty) "N/A" else dpto
  val provDisplay = if (prov.isEmpty) "N/A" else prov
  val distDisplay = if (dist.isEmpty) "N/A" else dist
  
  println(f"$count%,10d | ${dptoDisplay}%-20s | ${provDisplay}%-20s | ${distDisplay}%-20s")
}

// Analisis adicional: Top 10 Departamentos
println("\n[4] TOP 10 DEPARTAMENTOS:")
println("-" * 50)

val topDepartamentos = datosParseados
  .map(registro => (registro._10, 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)
  .take(10)

topDepartamentos.foreach { case (dpto, count) =>
  val dptoDisplay = if (dpto.isEmpty) "No especificado" else dpto
  println(f"${dptoDisplay}%-30s : $count%,10d visitas")
}

// Analisis: Top 10 Distritos de Lima
println("\n[5] TOP 10 DISTRITOS DE LIMA:")
println("-" * 50)

val distritosLima = datosParseados
  .filter(registro => registro._11.toUpperCase.contains("LIMA"))
  .map(registro => (registro._12, 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)
  .take(10)

distritosLima.foreach { case (distrito, count) =>
  val distDisplay = if (distrito.isEmpty) "No especificado" else distrito
  println(f"${distDisplay}%-30s : $count%,10d visitas")
}

// Guardar resultado
println("\n[6] Guardando resultado...")
val resultado = visitasPorUbicacion.map { case (count, dpto, prov, dist) =>
  s"$count,$dpto,$prov,$dist"
}

val headerCSV = sc.parallelize(Seq("Visitas,Departamento,Provincia,Distrito"))
headerCSV.union(resultado).coalesce(1).saveAsTextFile("output/consulta_2_ubicacion_geografica")

println("- Resultado guardado en: output/consulta_2_ubicacion_geografica")
println("\n" + "=" * 70)
println("CONSULTA 2 COMPLETADA")
println("=" * 70)