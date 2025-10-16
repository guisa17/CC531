package Consulta7;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * Reducer – Calcula el promedio del importe por entidad y clasifica según rangos
 */
public class ClassificationReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, Text> {

    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        double suma = 0;
        int contador = 0;

        while (values.hasNext()) {
            suma += values.next().get();
            contador++;
        }

        double promedio = (contador > 0) ? (suma / contador) : 0.0;
        String categoria;

        if (promedio >= 300000) {
            categoria = "Alta inversión";
        } else if (promedio >= 100000) {
            categoria = "Media inversión";
        } else {
            categoria = "Baja inversión";
        }

        String resultado = String.format("Promedio=%.2f, Clase=%s", promedio, categoria);
        output.collect(key, new Text(resultado));
    }
}
