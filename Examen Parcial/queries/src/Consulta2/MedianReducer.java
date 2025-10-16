package Consulta2;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * CONSULTA 2 - REDUCER
 * Calcula la mediana de los valores recibidos
 */
public class MedianReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterator<DoubleWritable> values,
                       OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

        List<Double> listaValores = new ArrayList<>();

        while (values.hasNext()) {
            listaValores.add(values.next().get());
        }

        if (listaValores.isEmpty()) {
            output.collect(key, new DoubleWritable(0.0));
            return;
        }

        Collections.sort(listaValores);
        int n = listaValores.size();
        double mediana;

        if (n % 2 == 0) {
            // promedio de los dos del medio
            mediana = (listaValores.get(n / 2 - 1) + listaValores.get(n / 2)) / 2.0;
        } else {
            // elemento del medio
            mediana = listaValores.get(n / 2);
        }

        output.collect(key, new DoubleWritable(mediana));
    }
}
