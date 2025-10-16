package Consulta7;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * Mapper – Clasificación por rangos de inversión (IMPORTE_FONDECYT)
 * Lee cada registro y emite (ENTIDAD_EJECUTORA, IMPORTE_FONDECYT)
 */
public class ClassificationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException {

        // Ignorar cabecera
        if (key.get() == 0) return;

        String[] cols = value.toString().split(";");

        try {
            String entidad = cols[8].trim(); // ENTIDAD_EJECUTORA_SUBVENCIONADO
            double importe = Double.parseDouble(cols[14].trim()); // IMPORTE_FONDECYT

            output.collect(new Text(entidad), new DoubleWritable(importe));

        } catch (NumberFormatException e) {
            reporter.incrCounter("Consulta7", "ImporteInvalido", 1);
        }
    }
}
