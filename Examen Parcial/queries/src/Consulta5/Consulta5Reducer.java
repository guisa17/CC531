package Consulta5;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * REDUCERS de la Consulta 5
 * Promedio del IMPORTE_FONDECYT por TIPO_ENTIDAD y SEXO
 */

public class Consulta5Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double suma = 0.0;
        int count = 0;

        while (values.hasNext()) {
            String[] val = values.next().toString().split(",");
            suma += Double.parseDouble(val[0]);
            count += Integer.parseInt(val[1]);
        }

        // Output parcial: (TIPO_ENTIDAD,SEXO)  suma,count
        output.collect(key, new Text(suma + "," + count));
    }
}


class Consulta5Reducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double sumaTotal = 0.0;
        int countTotal = 0;

        while (values.hasNext()) {
            String[] val = values.next().toString().split(",");
            sumaTotal += Double.parseDouble(val[0]);
            countTotal += Integer.parseInt(val[1]);
        }

        // Output combinado: (TIPO_ENTIDAD,SEXO)  sumaTotal,countTotal
        output.collect(key, new Text(sumaTotal + "," + countTotal));
    }
}


class Consulta5Reducer3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double sumaFinal = 0.0;
        int countFinal = 0;

        while (values.hasNext()) {
            String[] val = values.next().toString().split(",");
            sumaFinal += Double.parseDouble(val[0]);
            countFinal += Integer.parseInt(val[1]);
        }

        // Calcular promedio con dos decimales
        double promedio = (countFinal == 0) ? 0.0 : sumaFinal / countFinal;
        DecimalFormat df = new DecimalFormat("#.00");

        output.collect(key, new Text(df.format(promedio)));
    }
}
