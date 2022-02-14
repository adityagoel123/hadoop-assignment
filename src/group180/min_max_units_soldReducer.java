package group180;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;



public class min_max_units_soldReducer extends 
        Reducer<Text, DoubleWritable, Text, String> 
{

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, 
            Context context) throws IOException, InterruptedException {
        
        // Standard algorithm for finding the average value
        double max_value = 0;
        double min_value = 9999999;
        double counter = 0;
        for (DoubleWritable value : values) {
        	counter=counter+1;
        	max_value = Math.max(max_value , value.get());
        	min_value = Math.min(min_value , value.get());
        }
        //double average_value=sum_value/counter;
        context.write(key, String.valueOf(max_value) + "," +String.valueOf(min_value));
    }
}