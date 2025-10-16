package Consulta4;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * REDUCERS - CONSULTA 4
 * Porcentaje del MONTO total por MONEDA dentro de cada ENTIDAD_EJECUTORA_SUBVENCIONADO
 */
public class Consulta4Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    // ===========================
    // REDUCER 1
    // ===========================
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double suma = 0.0;
        while (values.hasNext()) {
            suma += Double.parseDouble(values.next().toString());
        }
        output.collect(key, new Text(String.valueOf(suma)));
    }
}

// ===========================
// REDUCER 2
// ===========================
class Consulta4Reducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double total = 0.0;
        while (values.hasNext()) {
            total += Double.parseDouble(values.next().toString());
        }
        output.collect(key, new Text(String.valueOf(total)));
    }
}

// ===========================
// REDUCER 3
// ===========================
class Consulta4Reducer3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        // Este tercer reduce puede calcular los porcentajes finales.
        // Ejemplo de entrada combinada: "PUCP,S/." 900000 , "PUCP" 1000000
        // Pero aquí suponemos que la data del total está ya disponible en paralelo.
        // En Hadoop real usarías un DistributedCache; aquí simulamos proporciones por clave.

        // Como simplificación, este Reducer puede asumir que la proporción se calcula dentro del mismo job:
        double totalEntidad = 0.0;
        List<Double> montos = new ArrayList<>();
        while (values.hasNext()) {
            double v = Double.parseDouble(values.next().toString());
            montos.add(v);
            totalEntidad += v;
        }

        DecimalFormat df = new DecimalFormat("#.00");
        for (double m : montos) {
            double porcentaje = (totalEntidad == 0) ? 0 : (m / totalEntidad) * 100;
            output.collect(key, new Text(df.format(porcentaje) + "%"));
        }
    }
}
