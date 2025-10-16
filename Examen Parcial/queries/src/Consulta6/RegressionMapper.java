package Consulta6;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * REGRESIÓN LINEAL (Consulta 6)
 * Modelo: Y = β0 + β1 * X
 * Donde:
 *   X = MONTO
 *   Y = IMPORTE_FONDECYT
 */
public class RegressionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    // ===========================
    // MAPPER 1 - Estadísticas base
    // ===========================
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return; // saltar encabezado

        String[] fields = value.toString().split(";");
        if (fields.length > 15) {
            try {
                double X = Double.parseDouble(fields[14]); // MONTO
                double Y = Double.parseDouble(fields[15]); // IMPORTE_FONDECYT
                double X2 = X * X;
                double Y2 = Y * Y;
                double XY = X * Y;
                output.collect(new Text("stats"), new Text(X + "," + Y + "," + X2 + "," + Y2 + "," + XY + ",1"));
            } catch (Exception ignored) {}
        }
    }
}

// ===========================
// MAPPER 2 - Lectura de sumas para coeficientes
// ===========================
class RegressionMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            output.collect(new Text("coef"), new Text(parts[1]));
        }
    }
}

// ===========================
// MAPPER 3 - Predicciones
// ===========================
class RegressionMapper3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;

        String[] fields = value.toString().split(";");
        if (fields.length > 14) {
            try {
                double X = Double.parseDouble(fields[14]);
                output.collect(new Text("predict"), new Text(String.valueOf(X)));
            } catch (Exception ignored) {}
        }
    }
}
