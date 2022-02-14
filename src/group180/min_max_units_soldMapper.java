package group180;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * This is the main Mapper class. 
 * 
 * @author Group 180
 */
public class min_max_units_soldMapper extends 
        Mapper<LongWritable, Text, Text, DoubleWritable> 
{

    @Override
    public void map(LongWritable key, Text value, Context context) throws 
            IOException, InterruptedException {

        String[] line = value.toString().split(",", 15);
        
        Configuration conf = context.getConfiguration();
        String item_type = conf.get("item_type");    
	    
        if (line[3].equals(item_type)) {

        	//The output `key` is the name of the region
		    String outputKey = line[7] + "," + item_type + "," + line[2] + ",";
		
		    //The output `value` is the magnitude of the earthquake
		    double outputValue = Double.parseDouble(line[10]);
		    
		    // Record the output in the Context object
		    context.write(new Text(outputKey), new DoubleWritable(outputValue));
        }
    }
}