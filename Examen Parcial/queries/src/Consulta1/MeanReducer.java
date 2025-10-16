package Consulta1;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
public class MeanReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    
    public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double sumaCasos = 0;
        int cont=0;
        while (values.hasNext()) {
            
            DoubleWritable value = (DoubleWritable) values.next();
            sumaCasos += value.get();
            cont=cont+1;
            
        }

        double mean=(double)sumaCasos/cont;
        output.collect(key, new DoubleWritable(mean));
    }
}