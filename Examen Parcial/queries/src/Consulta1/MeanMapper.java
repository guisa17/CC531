package Consulta1;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class MeanMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    //private final static IntWritable one = new IntWritable(1);
    DoubleWritable numero;
    Text txttotal= new Text("MediaEs");
    
    
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] ProjectData = valueString.split(";");        
        
        //Cual es la media del importe recibido
        
        try{
            double casos=Double.parseDouble(ProjectData[14]);
            numero=new DoubleWritable(casos);
        } catch (NumberFormatException e){
            numero=new DoubleWritable(0);
            reporter.incrCounter("Consulta1","Excepciones",1);
        }
        
        reporter.incrCounter("Consulta1","NumFilas",1);
        output.collect(txttotal,numero);         
        
        
    }
}