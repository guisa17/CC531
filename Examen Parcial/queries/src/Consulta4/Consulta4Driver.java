package Consulta4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * DRIVER - CONSULTA 4
 * Encadena los 3 MapReduce Jobs para calcular el porcentaje del MONTO
 * por MONEDA dentro de cada ENTIDAD_EJECUTORA_SUBVENCIONADO
 */

public class Consulta4Driver {

    public static void main(String[] args) {

        // Verificación de argumentos
        if (args.length < 4) {
            System.err.println("Uso: Consulta4Driver <input> <output1> <output2> <output3>");
            System.exit(-1);
        }

        try {
            // ======================
            // JOB 1 - suma por entidad y moneda
            // ======================
            JobConf job_conf1 = new JobConf(Consulta4Driver.class);
            job_conf1.setJobName("Consulta4_MapReduce1");
            job_conf1.setOutputKeyClass(Text.class);
            job_conf1.setOutputValueClass(Text.class);
            job_conf1.setMapperClass(Consulta4.Consulta4Mapper.class);
            job_conf1.setReducerClass(Consulta4.Consulta4Reducer.class);
            job_conf1.setInputFormat(TextInputFormat.class);
            job_conf1.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job_conf1, new Path(args[1]));

            JobClient.runJob(job_conf1);

            // ======================
            // JOB 2 - total por entidad
            // ======================
            JobConf job_conf2 = new JobConf(Consulta4Driver.class);
            job_conf2.setJobName("Consulta4_MapReduce2");
            job_conf2.setOutputKeyClass(Text.class);
            job_conf2.setOutputValueClass(Text.class);
            job_conf2.setMapperClass(Consulta4.Consulta4Mapper2.class);
            job_conf2.setReducerClass(Consulta4.Consulta4Reducer2.class);
            job_conf2.setInputFormat(TextInputFormat.class);
            job_conf2.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job_conf2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job_conf2, new Path(args[2]));

            JobClient.runJob(job_conf2);

            // ======================
            // JOB 3 - porcentaje por moneda
            // ======================
            JobConf job_conf3 = new JobConf(Consulta4Driver.class);
            job_conf3.setJobName("Consulta4_MapReduce3");
            job_conf3.setOutputKeyClass(Text.class);
            job_conf3.setOutputValueClass(Text.class);
            job_conf3.setMapperClass(Consulta4.Consulta4Mapper3.class);
            job_conf3.setReducerClass(Consulta4.Consulta4Reducer3.class);
            job_conf3.setInputFormat(TextInputFormat.class);
            job_conf3.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job_conf3, new Path(args[2]));
            FileOutputFormat.setOutputPath(job_conf3, new Path(args[3]));

            JobClient.runJob(job_conf3);

            System.out.println("Consulta 4 ejecutada correctamente con 3 MapReduce Jobs");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
