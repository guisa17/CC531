package Consulta5;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * DRIVER - Consulta 5
 * Ejecuta los 3 MapReduce para calcular el promedio del IMPORTE_FONDECYT
 * agrupado por TIPO_ENTIDAD y SEXO.
 */

public class Consulta5Driver {

    public static void main(String[] args) {

        if (args.length < 4) {
            System.err.println("Uso: Consulta5Driver <input> <output1> <output2> <output3>");
            System.exit(-1);
        }

        try {
            // ======================
            // JOB 1
            // ======================
            JobConf job_conf1 = new JobConf(Consulta5Driver.class);
            job_conf1.setJobName("Consulta5_MapReduce1");
            job_conf1.setOutputKeyClass(Text.class);
            job_conf1.setOutputValueClass(Text.class);
            job_conf1.setMapperClass(Consulta5.Consulta5Mapper.class);
            job_conf1.setReducerClass(Consulta5.Consulta5Reducer.class);
            job_conf1.setInputFormat(TextInputFormat.class);
            job_conf1.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job_conf1, new Path(args[1]));

            JobClient.runJob(job_conf1);

            // ======================
            // JOB 2
            // ======================
            JobConf job_conf2 = new JobConf(Consulta5Driver.class);
            job_conf2.setJobName("Consulta5_MapReduce2");
            job_conf2.setOutputKeyClass(Text.class);
            job_conf2.setOutputValueClass(Text.class);
            job_conf2.setMapperClass(Consulta5.Consulta5Mapper2.class);
            job_conf2.setReducerClass(Consulta5.Consulta5Reducer2.class);
            job_conf2.setInputFormat(TextInputFormat.class);
            job_conf2.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job_conf2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job_conf2, new Path(args[2]));

            JobClient.runJob(job_conf2);

            // ======================
            // JOB 3
            // ======================
            JobConf job_conf3 = new JobConf(Consulta5Driver.class);
            job_conf3.setJobName("Consulta5_MapReduce3");
            job_conf3.setOutputKeyClass(Text.class);
            job_conf3.setOutputValueClass(Text.class);
            job_conf3.setMapperClass(Consulta5.Consulta5Mapper3.class);
            job_conf3.setReducerClass(Consulta5.Consulta5Reducer3.class);
            job_conf3.setInputFormat(TextInputFormat.class);
            job_conf3.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job_conf3, new Path(args[2]));
            FileOutputFormat.setOutputPath(job_conf3, new Path(args[3]));

            JobClient.runJob(job_conf3);

            System.out.println("Ejecución completa de los 3 MapReduce (Consulta 5)");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
