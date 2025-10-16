package Consulta7;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * Driver – Configura y ejecuta el Job de clasificación por rangos de inversión
 */
public class ClassificationDriver {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Uso: ClassificationDriver <input> <output>");
            System.exit(-1);
        }

        JobConf conf = new JobConf(ClassificationDriver.class);
        conf.setJobName("Consulta7_ClasificaciónPorRangos");

        conf.setMapperClass(Consulta7.ClassificationMapper.class);
        conf.setReducerClass(Consulta7.ClassificationReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
