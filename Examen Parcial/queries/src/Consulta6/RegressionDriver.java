package Consulta6;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * DRIVER - Consulta 6 (Regresión Lineal)
 */
public class RegressionDriver {

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Uso: RegressionDriver <input> <output1> <output2> <output3>");
            System.exit(-1);
        }

        try {
            // ======================
            // JOB 1 - Calcular sumas
            // ======================
            JobConf job1 = new JobConf(RegressionDriver.class);
            job1.setJobName("Regression_MapReduce1");
            job1.setMapperClass(Consulta6.RegressionMapper.class);
            job1.setReducerClass(Consulta6.RegressionReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            job1.setInputFormat(TextInputFormat.class);
            job1.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));
            JobClient.runJob(job1);

            // ======================
            // JOB 2 - Calcular coeficientes
            // ======================
            JobConf job2 = new JobConf(RegressionDriver.class);
            job2.setJobName("Regression_MapReduce2");
            job2.setMapperClass(Consulta6.RegressionMapper2.class);
            job2.setReducerClass(Consulta6.RegressionReducer2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormat(TextInputFormat.class);
            job2.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            JobClient.runJob(job2);

            // ======================
            // JOB 3 - Predicciones
            // ======================
            JobConf job3 = new JobConf(RegressionDriver.class);
            job3.setJobName("Regression_MapReduce3");
            job3.setMapperClass(Consulta6.RegressionMapper3.class);
            job3.setReducerClass(Consulta6.RegressionReducer3.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            job3.setInputFormat(TextInputFormat.class);
            job3.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job3, new Path(args[0]));
            FileOutputFormat.setOutputPath(job3, new Path(args[3]));
            JobClient.runJob(job3);

            System.out.println("Regresión Lineal ejecutada correctamente (Consulta 6)");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
