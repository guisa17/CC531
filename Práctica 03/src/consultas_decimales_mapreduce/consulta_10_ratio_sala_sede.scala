println("=" * 70)
println("CONSULTA 10: Ratio de Visitas por Sala respecto a su Sede")
println("=" * 70)

println("\nPregunta: ¿Que proporcion de visitas de cada sede corresponde a cada sala?")
println("Campos utilizados: VISITA_SEDE, VISITA_SALA")
println("MapReduce: 3 niveles anidados con decimales")

// Cargar datos limpios
val datosConsulta10 = sc.objectFile[(String, String, String, String, String, String,
                           String, String, String, String, String, String)](
                           "output/datos_limpios_rdd")

println(s"\n[1] Datos cargados: ${datosConsulta10.count()} registros")

// Filtrar registros con sede y sala validas
val datosFiltrados = datosConsulta10.filter { r =>
  r._2 != null && r._2.trim.nonEmpty &&
  r._3 != null && r._3.trim.nonEmpty
}

println(s"Registros validos: ${datosFiltrados.count()}")

// MR1: Reduce - Contar por sala y sede
println("\n[2] MR1: Conteo de visitas por sala y sede...")

val mr1 = datosFiltrados
  .map { r => ((r._2, r._3), 1) }
  .reduceByKey(_ + _)

println(s"MR1 generado: ${mr1.count()} registros")

// MR2: Reduce - Total por sede
println("\n[3] MR2: Calculando total de visitas por sede...")

val mr2 = mr1.map { case ((sede, sala), count) =>
  (sede, count)
}.reduceByKey(_ + _)

println(s"MR2 generado: ${mr2.count()} registros")

// MR3: Map - Calcular ratio (decimal)
println("\n[4] MR3: Calculando ratio sala/sede...")

val totalPorSede = mr2.collect().toMap
val br_totalPorSede = sc.broadcast(totalPorSede)

val mr3 = mr1.map { case ((sede, sala), countSala) =>
  val total = br_totalPorSede.value.getOrElse(sede, countSala)
  val ratio = countSala.toDouble / total
  (sede, sala, countSala, ratio)
}.sortBy(_._4, ascending = false)

println("\n[5] Resultados:")
println("-" * 90)
println(f"${"Sede"}%-25s | ${"Sala"}%-35s | ${"Visitas"}%-8s | ${"Ratio"}%-8s")
println("-" * 90)

mr3.collect().foreach { case (sede, sala, countSala, ratio) =>
  println(f"$sede%-25s | $sala%-35s | $countSala%8d | $ratio%6.4f")
}

// Guardar resultados
val resultado10 = mr3.map { case (sede, sala, countSala, ratio) =>
  f"$sede,$sala,$countSala,$ratio%.4f"
}

val header10 = sc.parallelize(Seq("Sede,Sala,Visitas,Ratio"))
header10.union(resultado10).coalesce(1).saveAsTextFile("output/consulta_10_ratio_sala_sede")

println("\n[6] Resultados guardados en: output/consulta_10_ratio_sala_sede/")
println("=" * 70)
