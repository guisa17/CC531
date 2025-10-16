package Consulta3;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class StdevReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

        double suma = 0.0;
        double sumaCuadrados = 0.0;
        int n = 0;

        while (values.hasNext()) {
            double valor = values.next().get();
            suma += valor;
            sumaCuadrados += valor * valor;
            n++;
        }

        if (n == 0) {
            output.collect(key, new DoubleWritable(0.0));
            return;
        }

        // Media
        double media = suma / n;
        // Varianza = (Σx² / n) - μ²
        double varianza = (sumaCuadrados / n) - (media * media);
        // Desviación estándar
        double desviacionEstandar = Math.sqrt(varianza);

        output.collect(key, new DoubleWritable(desviacionEstandar));
    }
}
