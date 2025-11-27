println("=" * 70)
println("CONSULTA 3: Analisis Temporal por Sede, Mes y Dia de Semana")
println("=" * 70)

println("\nPregunta: ¿En que meses y dias de la semana recibe mas visitas cada sede?")
println("Campos utilizados: VISITA_SEDE, VISITA_FECHAVISITA (Mes), VISITA_FECHAVISITA (Dia)")

// Cargar datos limpios
type RegistroBNP = (String, String, String, String, String, String, 
                    String, String, String, String, String, String)

val datosParseados = sc.objectFile[RegistroBNP]("output/datos_limpios_rdd")

println(s"\n[1] Datos cargados: ${datosParseados.count()} registros")

// Función auxiliar: Extraer mes y día de semana de fecha (formato: YYYYMMDD)
def extraerMes(fecha: String): String = {
  if (fecha.length >= 6) fecha.substring(4, 6) else "00"
}

def extraerDiaSemana(fecha: String): String = {
  // Convertir YYYYMMDD a día de semana (simplificado)
  // Usamos módulo 7 para simular día de semana
  if (fecha.length == 8) {
    try {
      val dia = fecha.substring(6, 8).toInt
      val mes = fecha.substring(4, 6).toInt
      val anno = fecha.substring(0, 4).toInt
      
      // Algoritmo simple para calcular día de semana (Zeller's congruence simplificado)
      val diasSemana = Array("Domingo", "Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado")
      val h = (dia + 13 * (mes + 1) / 5 + anno + anno / 4 - anno / 100 + anno / 400) % 7
      diasSemana(h)
    } catch {
      case _: Exception => "Desconocido"
    }
  } else "Desconocido"
}

// MapReduce: Agrupar por (Sede, Mes, DiaSemana) y contar
println("\n[2] Aplicando MapReduce con extraccion de fecha...")

val visitasPorSedeMesDia = datosParseados
  .map { registro =>
    val sede = registro._2          // VISITA_SEDE
    val fechaVisita = registro._4   // VISITA_FECHAVISITA
    val mes = extraerMes(fechaVisita)
    val diaSemana = extraerDiaSemana(fechaVisita)
    
    // Map: Crear clave compuesta (sede, mes, diaSemana) con valor 1
    ((sede, mes, diaSemana), 1)
  }
  .reduceByKey(_ + _)  // Reduce: Sumar visitas
  .map { case ((sede, mes, dia), count) =>
    (count, sede, mes, dia)  // Invertir para ordenar
  }
  .sortBy(_._1, ascending = false)

// Mostrar Top 25
println("\n[3] TOP 25 COMBINACIONES (Sede + Mes + Dia Semana):")
println("-" * 80)
println(f"${"Visitas"}%-10s | ${"Sede"}%-35s | ${"Mes"}%-4s | ${"Dia Semana"}%-12s")
println("-" * 80)

val nombresMeses = Map(
  "01" -> "Ene", "02" -> "Feb", "03" -> "Mar", "04" -> "Abr",
  "05" -> "May", "06" -> "Jun", "07" -> "Jul", "08" -> "Ago",
  "09" -> "Sep", "10" -> "Oct", "11" -> "Nov", "12" -> "Dic"
)

visitasPorSedeMesDia.take(25).foreach { case (count, sede, mes, dia) =>
  val mesNombre = nombresMeses.getOrElse(mes, mes)
  println(f"$count%,10d | ${sede}%-35s | ${mesNombre}%-4s | ${dia}%-12s")
}

// Analisis adicional: Visitas por mes (consolidado)
println("\n[4] DISTRIBUCION POR MES (consolidado):")
println("-" * 40)

val visitasPorMes = datosParseados
  .map(registro => (extraerMes(registro._4), 1))
  .reduceByKey(_ + _)
  .sortBy(_._1)

visitasPorMes.collect().foreach { case (mes, count) =>
  val mesNombre = nombresMeses.getOrElse(mes, mes)
  println(f"${mesNombre}%-10s : $count%,10d visitas")
}

// Analisis adicional: Visitas por día de semana (consolidado)
println("\n[5] DISTRIBUCION POR DIA DE SEMANA (consolidado):")
println("-" * 40)

val ordenDias = Map(
  "Lunes" -> 1, "Martes" -> 2, "Miercoles" -> 3, "Jueves" -> 4,
  "Viernes" -> 5, "Sabado" -> 6, "Domingo" -> 7
)

val visitasPorDia = datosParseados
  .map(registro => (extraerDiaSemana(registro._4), 1))
  .reduceByKey(_ + _)
  .sortBy(x => ordenDias.getOrElse(x._1, 99))

visitasPorDia.collect().foreach { case (dia, count) =>
  println(f"${dia}%-12s : $count%,10d visitas")
}

// Guardar resultado
println("\n[6] Guardando resultado...")
val resultado = visitasPorSedeMesDia.map { case (count, sede, mes, dia) =>
  s"$count,$sede,$mes,$dia"
}

val headerCSV = sc.parallelize(Seq("Visitas,Sede,Mes,DiaSemana"))
headerCSV.union(resultado).coalesce(1).saveAsTextFile("output/consulta_3_sede_mes_dia")

println("- Resultado guardado en: output/consulta_3_sede_mes_dia")
println("\n" + "=" * 70)
println("CONSULTA 3 COMPLETADA")
println("=" * 70)