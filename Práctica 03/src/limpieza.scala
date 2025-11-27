println("=" * 60)
println("PARTE 1: CARGA Y LIMPIEZA DE DATOS")
println("=" * 60)

// 1. CARGA DE DATOS
println("\n[1] Cargando datos desde CSV...")
val dataBNP = sc.textFile("data/data_bnp.csv")

// 2. SEPARAR HEADER
val header = dataBNP.first()
println(s"\nHeader: $header")

val dataSinHeader = dataBNP.filter(line => line != header)

println(s"\nTotal de líneas (con header): ${dataBNP.count()}")
println(s"Total de registros (sin header): ${dataSinHeader.count()}")

// 3. LIMPIEZA: ELIMINAR FILAS VACÍAS
println("\n[2] Eliminando filas vacías...")
val dataSinVacios = dataSinHeader.filter(line => {
  val campos = line.split(",", -1)
  // Verificar que no todos los campos estén vacíos
  campos.exists(campo => campo.trim.nonEmpty)
})

println(s"Registros después de eliminar vacíos: ${dataSinVacios.count()}")

// 4. LIMPIEZA: ELIMINAR DUPLICADOS
println("\n[3] Eliminando duplicados...")
val dataSinDuplicados = dataSinVacios.distinct()

println(s"Registros después de eliminar duplicados: ${dataSinDuplicados.count()}")

// 5. VALIDACIÓN: VERIFICAR ESTRUCTURA
println("\n[4] Verificando estructura de datos...")
val primerasLineas = dataSinDuplicados.take(3)
primerasLineas.foreach(println)

// Contar campos por línea
val camposPorLinea = dataSinDuplicados.map(line => line.split(",", -1).length)
val numCamposEsperados = header.split(",", -1).length

println(s"\nNúmero de campos esperados: $numCamposEsperados")
println(s"Distribución de campos:")
camposPorLinea.map(n => (n, 1))
  .reduceByKey(_ + _)
  .sortBy(_._1)
  .collect()
  .foreach { case (numCampos, count) =>
    println(f"  $numCampos%2d campos: $count%,d registros")
  }

// 6. FILTRAR REGISTROS CON NÚMERO CORRECTO DE CAMPOS
println("\n[5] Filtrando registros con estructura correcta...")
val datosLimpios = dataSinDuplicados.filter(line => {
  line.split(",", -1).length == numCamposEsperados
})

println(s"Registros con estructura válida: ${datosLimpios.count()}")

// 7. CREAR RDD CON CAMPOS PARSEADOS
println("\n[6] Parseando campos...")
val datosParseados = datosLimpios.map(line => {
  val campos = line.split(",", -1)
  (
    campos(0).trim,  // FECHACORTE
    campos(1).trim,  // VISITA_SEDE
    campos(2).trim,  // VISITA_SALA
    campos(3).trim,  // VISITA_FECHAVISITA
    campos(4).trim,  // VISITA_HORAVISITA
    campos(5).trim,  // CIUDADANO_TIPO_DOCUMENTO
    campos(6).trim,  // CIUDADANO_DOCUMENTO_IDENTIDAD_ANONIMIZADO
    campos(7).trim,  // CIUDADANO_FECHA_NACIMIENTO
    campos(8).trim,  // CIUDADANO_SEXO
    campos(9).trim,  // CIUDADANO_DEPARTAMENTO
    campos(10).trim, // CIUDADANO_PROVINCIA
    campos(11).trim  // CIUDADANO_DISTRITO
  )
})

// Cache para reutilizar
datosParseados.cache()

println(s"Datos parseados en memoria: ${datosParseados.count()} registros")

// 8. ESTADÍSTICAS DE LIMPIEZA
println("\n[7] Verificando valores nulos por campo...")

val columnas = Array(
  "FECHACORTE", "VISITA_SEDE", "VISITA_SALA", "VISITA_FECHAVISITA",
  "VISITA_HORAVISITA", "CIUDADANO_TIPO_DOCUMENTO", 
  "CIUDADANO_DOCUMENTO_IDENTIDAD_ANONIMIZADO", "CIUDADANO_FECHA_NACIMIENTO",
  "CIUDADANO_SEXO", "CIUDADANO_DEPARTAMENTO", "CIUDADANO_PROVINCIA", 
  "CIUDADANO_DISTRITO"
)

println("\nValores vacíos por columna:")
for (i <- 0 until columnas.length) {
  val vacios = datosParseados.filter(registro => {
    val campo = registro.productElement(i).toString
    campo.isEmpty || campo.equals("null") || campo.equals("NULL")
  }).count()
  
  println(f"  ${columnas(i)}%-45s : $vacios%,d vacíos")
}

// 9. GUARDAR DATOS LIMPIOS
println("\n[8] Guardando datos limpios...")

// Opción 1: Guardar como texto CSV
val datosLimpiosCSV = datosParseados.map(registro => {
  s"${registro._1},${registro._2},${registro._3},${registro._4}," +
  s"${registro._5},${registro._6},${registro._7},${registro._8}," +
  s"${registro._9},${registro._10},${registro._11},${registro._12}"
})

// Agregar header
val datosConHeader = sc.parallelize(Seq(header)).union(datosLimpiosCSV)

datosConHeader.coalesce(1).saveAsTextFile("output/datos_limpios_csv")
println("- Datos guardados en: output/datos_limpios_csv")

// Opción 2: Guardar RDD parseado para análisis posteriores
datosParseados.saveAsObjectFile("output/datos_limpios_rdd")
println("- RDD parseado guardado en: output/datos_limpios_rdd")

// 10. RESUMEN FINAL
println("\n" + "=" * 60)
println("RESUMEN DE LIMPIEZA")
println("=" * 60)
println(f"Registros originales:           ${dataBNP.count() - 1}%,d")
println(f"Después de eliminar vacíos:     ${dataSinVacios.count()}%,d")
println(f"Después de eliminar duplicados: ${dataSinDuplicados.count()}%,d")
println(f"Después de validar estructura:  ${datosLimpios.count()}%,d")
println(f"Registros finales limpios:      ${datosParseados.count()}%,d")

val registrosEliminados = (dataBNP.count() - 1) - datosParseados.count()
val porcentajeEliminado = (registrosEliminados.toDouble / (dataBNP.count() - 1)) * 100
println(f"\nRegistros eliminados: $registrosEliminados%,d ($porcentajeEliminado%.2f%%)")
println("=" * 60)

// 11. MOSTRAR MUESTRA DE DATOS LIMPIOS
println("\n[9] Muestra de datos limpios (primeros 5 registros):")
println("\nFormato: (FECHACORTE, SEDE, SALA, FECHA_VISITA, HORA, ...)")
datosParseados.take(5).foreach(println)

println("\n- Limpieza completada exitosamente")
println("- Datos listos para análisis MapReduce")
