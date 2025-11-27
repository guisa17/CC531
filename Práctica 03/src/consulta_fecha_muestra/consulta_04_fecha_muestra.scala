println("=" * 70)
println("CONSULTA 4: Filtrado de Fechas con Muestra Aleatoria")
println("=" * 70)

println("\nPregunta: ¿Cuantas visitas hubo en un rango de fechas especifico usando una muestra aleatoria?")
println("Campos utilizados: VISITA_FECHAVISITA (filtrado por rango)")

// Cargar datos limpios
val datosConsulta4 = sc.objectFile[(String, String, String, String, String, String, 
                    String, String, String, String, String, String)]("output/datos_limpios_rdd")

println(s"\n[1] Datos cargados: ${datosConsulta4.count()} registros")

// Tomar una muestra aleatoria (30% de los datos)
println("\n[2] Tomando muestra aleatoria del 30%...")
val muestra = datosConsulta4.sample(withReplacement = false, fraction = 0.3, seed = 42)

println(s"Registros en la muestra: ${muestra.count()}")

// Definir rango de fechas (formato YYYYMMDD)
val fechaInicio = "20210101"  // 1 de enero de 2021
val fechaFin = "20211231"     // 31 de diciembre de 2021

println(s"\n[3] Filtrando por rango de fechas: $fechaInicio - $fechaFin")

// Filtrar muestra por rango de fechas
val visitasEnRango = muestra.filter { registro =>
  val fechaVisita = registro._4  // VISITA_FECHAVISITA
  fechaVisita >= fechaInicio && fechaVisita <= fechaFin
}

val totalEnRango = visitasEnRango.count()
println(s"Visitas en el rango (en la muestra): $totalEnRango")

// Analisis adicional: Distribucion por mes dentro del rango
println("\n[4] DISTRIBUCION POR MES EN EL RANGO:")
println("-" * 50)

val nombresMeses = Map(
  "01" -> "Enero", "02" -> "Febrero", "03" -> "Marzo", "04" -> "Abril",
  "05" -> "Mayo", "06" -> "Junio", "07" -> "Julio", "08" -> "Agosto",
  "09" -> "Septiembre", "10" -> "Octubre", "11" -> "Noviembre", "12" -> "Diciembre"
)

val visitasPorMes = visitasEnRango
  .map { registro =>
    val fecha = registro._4
    val mes = if (fecha.length >= 6) fecha.substring(4, 6) else "00"
    (mes, 1)
  }
  .reduceByKey(_ + _)
  .sortBy(_._1)

visitasPorMes.collect().foreach { case (mes, count) =>
  val mesNombre = nombresMeses.getOrElse(mes, mes)
  val porcentaje = (count.toDouble / totalEnRango) * 100
  println(f"${mesNombre}%-15s : $count%,8d visitas ($porcentaje%5.2f%%)")
}

// Analisis adicional: Top 5 sedes en el rango de fechas
println("\n[5] TOP 5 SEDES EN EL RANGO DE FECHAS:")
println("-" * 50)

val topSedes = visitasEnRango
  .map(registro => (registro._2, 1))  // VISITA_SEDE
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)
  .take(5)

topSedes.foreach { case (sede, count) =>
  val porcentaje = (count.toDouble / totalEnRango) * 100
  println(f"${sede}%-40s : $count%,8d ($porcentaje%5.2f%%)")
}

// Comparacion: Total en dataset completo vs muestra
println("\n[6] COMPARACION DATASET COMPLETO VS MUESTRA:")
println("-" * 50)

val totalCompletoEnRango = datosConsulta4.filter { registro =>
  val fechaVisita = registro._4
  fechaVisita >= fechaInicio && fechaVisita <= fechaFin
}.count()

val porcentajeMuestra = (muestra.count().toDouble / datosConsulta4.count()) * 100
val estimacionTotal = (totalEnRango.toDouble / 0.3).toLong

println(f"Total en dataset completo:     $totalCompletoEnRango%,10d visitas")
println(f"Total en muestra (30%%):        $totalEnRango%,10d visitas")
println(f"Estimacion a partir de muestra: $estimacionTotal%,10d visitas")
println(f"Error de estimacion:            ${math.abs(totalCompletoEnRango - estimacionTotal)}%,10d visitas")

// Guardar resultado
println("\n[7] Guardando resultado...")

val resultadoDetalle = visitasEnRango.map { registro =>
  s"${registro._1},${registro._2},${registro._3},${registro._4},${registro._5}"
}

val headerDetalle = sc.parallelize(Seq("FECHACORTE,VISITA_SEDE,VISITA_SALA,VISITA_FECHAVISITA,VISITA_HORAVISITA"))
headerDetalle.union(resultadoDetalle).coalesce(1).saveAsTextFile("output/consulta_4_fecha_muestra_2021")

println("- Resultado guardado en: output/consulta_4_fecha_muestra_2021")
println("\n" + "=" * 70)
println("CONSULTA 4 COMPLETADA")
println("=" * 70)
