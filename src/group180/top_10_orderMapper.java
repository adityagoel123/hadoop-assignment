package group180;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * This is the main Mapper class. 
 * 
 * @author Group 180
 */
public class top_10_orderMapper extends Mapper<Object,
Text, DoubleWritable, Text> {
		  
	@Override
    public void map(Object key, Text value, 
       Context context) throws IOException, 
                      InterruptedException
    {

        String[] line = value.toString().split(",", 16);
        
        
        Configuration conf = context.getConfiguration();
        String year = conf.get("year");     
	    
        if (line[7].equals(year)) {
        	
		    String outputKey = year + "," + line[8] + ",";
		
		    double outputValue = -1 * Double.parseDouble(line[15]);
        	
        	// insert into treeMap for auto sort
        	
		    //Orders order = new Orders(outputKey,outputValue);
		    //top_10_orders.add(order);
		    context.write(new DoubleWritable(outputValue),
                    new Text(outputKey));
        }
    }
}
