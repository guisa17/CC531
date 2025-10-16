package Consulta3;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class StdevMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final static Text keyOutput = new Text("StandardDeviation");
    private DoubleWritable numero = new DoubleWritable();

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] fields = line.split(";");

        // Cálculo de desviación estándar de los importes (columna 13)
        try {
            double importe = Double.parseDouble(fields[14]);  // Cambiado a double
            numero.set(importe);
        } catch (NumberFormatException e) {
            numero.set(0.0);
            reporter.incrCounter("Consulta3", "Excepciones", 1);
        }

        reporter.incrCounter("Consulta3", "NumFilas", 1);
        output.collect(keyOutput, numero);
    }
}
