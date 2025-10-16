package Consulta2;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * CONSULTA 2 - Cálculo de la MEDIANA
 * Obtiene la mediana de los valores de la columna 13 (decimales)
 */
public class MedianMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final static Text outputKey = new Text("Mediana");
    private final DoubleWritable numero = new DoubleWritable();

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException {

        if (key.get() == 0) return; // Evitar encabezado

        String[] fields = value.toString().split(";");

        try {
            double valor = Double.parseDouble(fields[14]); // Columna 13
            numero.set(valor);
            output.collect(outputKey, numero);
        } catch (NumberFormatException e) {
            reporter.incrCounter("Consulta2", "Valores no numéricos", 1);
        }
    }
}
