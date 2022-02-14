package group180;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;



public class tot_units_soldReducer extends 
        Reducer<Text, DoubleWritable, Text, DoubleWritable> 
{

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, 
            Context context) throws IOException, InterruptedException {
        
        // Standard algorithm for finding the average value
        double sum_value = 0;
        double counter = 0;
        for (DoubleWritable value : values) {
        	counter=counter+1;
        	sum_value = sum_value + value.get();
        }
        //double average_value=sum_value/counter;
        context.write(key, new DoubleWritable(sum_value));
    }
}