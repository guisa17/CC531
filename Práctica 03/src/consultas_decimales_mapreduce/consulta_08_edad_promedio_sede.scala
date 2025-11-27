println("=" * 70)
println("CONSULTA 8: Edad Promedio de Visitantes por Sede")
println("=" * 70)

println("\nPregunta: ¿Cual es la edad promedio de visitantes en cada sede?")
println("Campos utilizados: VISITA_SEDE, FECHACORTE, CIUDADANO_FECHA_NACIMIENTO")
println("MapReduce: 3 niveles anidados con decimales")

// Cargar datos limpios
val datosConsulta8 = sc.objectFile[(String, String, String, String, String, String,
                           String, String, String, String, String, String)](
                           "output/datos_limpios_rdd")

println(s"\n[1] Datos cargados: ${datosConsulta8.count()} registros")

// Filtrar registros validos
val datosFiltrados = datosConsulta8.filter { r =>
  val fecCorte = r._1
  val fecNac = r._8

  fecCorte != null && fecCorte.trim.nonEmpty &&
  fecNac != null && fecNac.trim.nonEmpty &&
  fecCorte.forall(_.isDigit) &&
  fecNac.forall(_.isDigit)
}

println(s"Registros validos despues del filtrado: ${datosFiltrados.count()}")

// MR1: Map - Calcular edad por persona
println("\n[2] MR1: Calculando edad individual...")

val mr1 = datosFiltrados.map { r =>
    val sede = r._2
    val fechaCorte = r._1.toInt
    val fechaNac = r._8.toInt
    val edad = (fechaCorte - fechaNac) / 10000.0
    (sede, (edad, 1))
}

println(s"MR1 generado: ${mr1.count()} registros")

// MR2: Reduce - Sumar edades y contar personas
println("\n[3] MR2: Agregando sumas y conteos por sede...")

val mr2 = mr1.reduceByKey { case ((sumA, countA), (sumB, countB)) =>
    (sumA + sumB, countA + countB)
}

println(s"MR2 generado: ${mr2.count()} registros")

// MR3: Map - Calcular promedio (decimal)
println("\n[4] MR3: Calculando edad promedio final...")

val mr3 = mr2.map { case (sede, (sumEdades, countPersonas)) =>
  val promedio = sumEdades / countPersonas
  (sede, promedio)
}.sortBy(_._2, ascending = false)

println("\n[5] Resultados:")
println("-" * 70)
println(f"${"Sede"}%-50s | ${"Edad Promedio"}%-15s")
println("-" * 70)

mr3.collect().foreach { case (sede, prom) =>
  println(f"$sede%-50s | $prom%5.2f años")
}

// Guardar resultados
val resultado8 = mr3.map { case (sede, prom) =>
  f"$sede,$prom%.2f"
}

val header8 = sc.parallelize(Seq("Sede,EdadPromedio"))
header8.union(resultado8).coalesce(1).saveAsTextFile("output/consulta_8_edad_promedio_sede")

println("\n[6] Resultados guardados en: output/consulta_8_edad_promedio_sede/")
println("=" * 70)
