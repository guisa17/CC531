package Consulta4;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * MAP REDUCE TRIPLE - CONSULTA 6
 * Porcentaje del MONTO total por MONEDA dentro de cada ENTIDAD_EJECUTORA_SUBVENCIONADO
 */
public class Consulta4Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    // ===========================
    // MAPPER 1
    // ===========================
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;

        String[] fields = value.toString().split(";");
        if (fields.length > 14) {
            String entidad = fields[8].trim();   // ENTIDAD_EJECUTORA_SUBVENCIONADO
            String moneda = fields[12].trim();   // MONEDA
            String montoStr = fields[14].trim(); // MONTO

            if (!montoStr.isEmpty() && montoStr.matches("\\d+(\\.\\d+)?")) {
                output.collect(new Text(entidad + "," + moneda), new Text(montoStr));
            }
        }
    }
}

// ===========================
// MAPPER 2
// ===========================
class Consulta4Mapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            String[] entidadMoneda = parts[0].split(",");
            String entidad = entidadMoneda[0].trim();
            String monto = parts[1].trim();
            output.collect(new Text(entidad), new Text(monto));
        }
    }
}

// ===========================
// MAPPER 3
// ===========================
class Consulta4Mapper3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        // Reenvía clave-valor tal cual para calcular porcentaje
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            output.collect(new Text(parts[0]), new Text(parts[1]));
        }
    }
}
