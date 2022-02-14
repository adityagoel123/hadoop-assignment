package group180;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;



public class top_10_orderReducer extends Reducer<DoubleWritable,
Text, Text, DoubleWritable> {

	//private TreeMap<Double, String> top_10_orders;
	  static int count;
	  
    public void setup(Context context) throws IOException,
                                     InterruptedException
    {
    	count=0;
    }

    public void reduce(DoubleWritable key, Iterable<Text> values,
    	      Context context) throws IOException, InterruptedException
    	    {
    	
    	double profit = (-1) * key.get();
    	String _value = null;
    	
    	for (Text value : values) 
        {
            _value = value.toString();
            	
	    	// we just write 10 records as output
	        if (count < 10)
	        {
	            context.write(new Text(_value),new DoubleWritable(profit));
	            count++;
	        }
        }
        
    }
}