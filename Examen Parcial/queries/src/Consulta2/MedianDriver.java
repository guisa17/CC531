package Consulta2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * CONSULTA 2 - DRIVER
 * Ejecuta el cálculo de la mediana con un solo MapReduce
 */
public class MedianDriver {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Uso: MedianDriver <input> <output>");
            System.exit(-1);
        }

        try {
            JobConf conf = new JobConf(MedianDriver.class);
            conf.setJobName("Consulta2_Mediana");

            conf.setMapperClass(Consulta2.MedianMapper.class);
            conf.setReducerClass(Consulta2.MedianReducer.class);

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(DoubleWritable.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));

            JobClient.runJob(conf);

            System.out.println("✅ Cálculo de la mediana ejecutado correctamente (Consulta 2)");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
