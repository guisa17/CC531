// cargar y limpiar datos
val datosSales = sc.textFile("salesjan/SalesJan2009.csv")
val datosSinHeader = datosSales.mapPartitionsWithIndex { (idx, iter) => 
    if (idx == 0) iter.drop(1) else iter 
}

// parsear la data
val datosParsed = datosSinHeader
    .map(_.split(","))
    .filter(_.length >= 8)
    .map { campos =>
        val producto = campos(1).trim
        val precio = campos(2).trim.replaceAll("\"", "").toDouble
        val pais = campos(7).trim
        ((pais, producto), precio)
    }

// sumar ingresos por (Price, Country)
val ingresosPorPaisProducto = datosParsed.reduceByKey(_ + _)

// producto con mayor ingreso por pais
val productoMaxPorPais = ingresosPorPaisProducto
    .map { case ((pais, producto), ingreso) => (pais, (producto, ingreso)) }
    .reduceByKey((a, b) => if (a._2 > b._2) a else b)
    .sortBy(_._2._2, ascending = false)

// resultados
println("\nResultados:")
println("-" * 70)
productoMaxPorPais.collect().foreach { case (pais, (producto, ingreso)) =>
    println(f"$pais%-30s | $producto%-15s | $$$ingreso%,.2f")
}
