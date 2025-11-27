println("=" * 70)
println("CONSULTA 9: Porcentaje de Visitas por Sexo en Cada Sede")
println("=" * 70)

println("\nPregunta: ¿Que porcentaje de visitantes son mujeres y hombres en cada sede?")
println("Campos utilizados: VISITA_SEDE, CIUDADANO_SEXO")
println("MapReduce: 3 niveles anidados con decimales")

// Cargar datos limpios
val datosConsulta9 = sc.objectFile[(String, String, String, String, String, String,
                           String, String, String, String, String, String)](
                           "output/datos_limpios_rdd")

println(s"\n[1] Datos cargados: ${datosConsulta9.count()} registros")

// Filtrar registros con sexo valido
val datosFiltrados = datosConsulta9.filter { r =>
  val sexo = r._9
  sexo != null && sexo.trim.nonEmpty && (sexo == "M" || sexo == "F")
}

println(s"Registros validos despues del filtrado: ${datosFiltrados.count()}")

// MR1: Reduce - Contar por sede y sexo
println("\n[2] MR1: Conteo de visitas por sede y sexo...")

val mr1 = datosFiltrados.map { r =>
  ((r._2, r._9), 1)
}.reduceByKey(_ + _)

println(s"MR1 generado: ${mr1.count()} registros")

// MR2: Reduce - Total por sede
println("\n[3] MR2: Calculando total de visitas por sede...")

val mr2 = mr1.map { case ((sede, sexo), count) =>
  (sede, count)
}.reduceByKey(_ + _)

println(s"MR2 generado: ${mr2.count()} registros")

// MR3: Map - Calcular porcentajes (decimal)
println("\n[4] MR3: Calculando porcentajes...")

val totalesPorSede = mr2.collect().toMap
val br_totales = sc.broadcast(totalesPorSede)

val mr3 = mr1.map { case ((sede, sexo), count) =>
  val total = br_totales.value.getOrElse(sede, count)
  val porcentaje = (count.toDouble / total) * 100.0
  (sede, sexo, porcentaje)
}.sortBy(_._3, ascending = false)

println("\n[5] Resultados:")
println("-" * 70)
println(f"${"Sede"}%-40s | ${"Sexo"}%-8s | ${"Porcentaje"}%-10s")
println("-" * 70)

mr3.collect().foreach { case (sede, sexo, porcentaje) =>
  val label = if (sexo == "F") "Mujeres" else "Hombres"
  println(f"$sede%-40s | $label%-8s | $porcentaje%6.2f %%")
}

// Guardar resultados
val resultado9 = mr3.map { case (sede, sexo, porcentaje) =>
  f"$sede,$sexo,$porcentaje%.2f"
}

val header9 = sc.parallelize(Seq("Sede,Sexo,Porcentaje"))
header9.union(resultado9).coalesce(1).saveAsTextFile("output/consulta_9_porcentaje_sexo_sede")

println("\n[6] Resultados guardados en: output/consulta_9_porcentaje_sexo_sede/")
println("=" * 70)
