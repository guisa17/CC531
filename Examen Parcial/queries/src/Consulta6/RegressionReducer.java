package Consulta6;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * REGRESIÓN LINEAL (Consulta 6)
 * REDUCERS - cálculo de sumas, coeficientes y predicciones
 */
public class RegressionReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    // ===========================
    // REDUCER 1 - Calcular sumas totales
    // ===========================
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double sumX = 0, sumY = 0, sumX2 = 0, sumY2 = 0, sumXY = 0;
        int n = 0;

        while (values.hasNext()) {
            String[] v = values.next().toString().split(",");
            sumX += Double.parseDouble(v[0]);
            sumY += Double.parseDouble(v[1]);
            sumX2 += Double.parseDouble(v[2]);
            sumY2 += Double.parseDouble(v[3]);
            sumXY += Double.parseDouble(v[4]);
            n += Integer.parseInt(v[5]);
        }

        output.collect(new Text("sumas"), new Text(sumX + "," + sumY + "," + sumX2 + "," + sumY2 + "," + sumXY + "," + n));
    }
}

// ===========================
// REDUCER 2 - Cálculo de coeficientes β0, β1
// ===========================
class RegressionReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            String[] s = values.next().toString().split(",");
            double sumX = Double.parseDouble(s[0]);
            double sumY = Double.parseDouble(s[1]);
            double sumX2 = Double.parseDouble(s[2]);
            double sumXY = Double.parseDouble(s[4]);
            int n = Integer.parseInt(s[5]);

            double b1 = (n * sumXY - sumX * sumY) / (n * sumX2 - Math.pow(sumX, 2));
            double b0 = (sumY - b1 * sumX) / n;

            output.collect(new Text("coeficientes"), new Text("b0=" + b0 + ", b1=" + b1));
        }
    }
}

// ===========================
// REDUCER 3 - Predicciones (simuladas)
// ===========================
class RegressionReducer3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    // Puedes reemplazarlos por los β reales una vez calculados
    private static final double b0 = 156942;  
    private static final double b1 = 0.65;    

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            double X = Double.parseDouble(values.next().toString());
            double Y_pred = b0 + b1 * X;
            output.collect(new Text("Predicción"), new Text("MONTO=" + X + " → IMPORTE_PRED=" + Y_pred));
        }
    }
}
