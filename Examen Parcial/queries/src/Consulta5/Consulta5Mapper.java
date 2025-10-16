package Consulta5;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * MAP REDUCE TRIPLE - CONSULTA 5
 * Promedio del IMPORTE_FONDECYT por TIPO_ENTIDAD y SEXO
 * 
 * Estructura:
 * - Mapper 1: Lee dataset original y emite (TIPO_ENTIDAD, SEXO) → (IMPORTE_FONDECYT, 1)
 * - Mapper 2: Lee salida parcial y reemite (TIPO_ENTIDAD, SEXO) → (SUMA, COUNT)
 * - Mapper 3: Lee salida final y reemite (TIPO_ENTIDAD, SEXO) → (SUMA_TOTAL, COUNT_TOTAL)
 */

public class Consulta5Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        // Saltar cabecera
        if (key.get() == 0) return;

        String[] fields = value.toString().split(";");
        if (fields.length > 15) {
            String tipoEntidad = fields[9].trim();  // Columna TIPO_ENTIDAD
            String sexo = fields[10].trim();         // Columna SEXO
            String importeStr = fields[14].trim();   // Columna IMPORTE_FONDECYT

            // Validar que el importe sea numérico
            if (!importeStr.isEmpty() && importeStr.matches("\\d+(\\.\\d+)?")) {
                // Ejemplo: ("Universidad Pública,Femenino", "182490.00,1")
                output.collect(new Text(tipoEntidad + "," + sexo), new Text(importeStr + ",1"));
            }
        }
    }
}

class Consulta5Mapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        // Lee línea del output del primer MR
        // Ejemplo: "Universidad Pública,Femenino\t182490.0,3"
        String[] parts = value.toString().split("\t");

        if (parts.length == 2) {
            String clave = parts[0].trim();
            String valor = parts[1].trim();
            output.collect(new Text(clave), new Text(valor));
        }
    }
}

class Consulta5Mapper3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        // Lee la salida del segundo MR (suma_total,count_total)
        String[] parts = value.toString().split("\t");

        if (parts.length == 2) {
            String clave = parts[0].trim();
            String valor = parts[1].trim();
            output.collect(new Text(clave), new Text(valor));
        }
    }
}
