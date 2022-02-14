package group180;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This is the main Partitioner class. 
 * 
 * @author Group 180
 */
public class min_max_units_soldPartitioner<K2, V2> extends Partitioner<Text,DoubleWritable>{
    public int getPartition(Text key, DoubleWritable value, int numReduceTasks){
    	
    	String[] line = key.toString().split(",", 2);
    	
    if(line[0].equals("2013"))
        return 1;
    if(line[0].equals("2014"))
        return 2;
    if(line[0].equals("2015"))
        return 3;
    if(line[0].equals("2016"))
        return 4;
    if(line[0].equals("2017"))
        return 5;
    if(line[0].equals("2018"))
        return 6;
    if(line[0].equals("2019"))
        return 7;
    if(line[0].equals("2020"))
        return 8;
    if(line[0].equals("2021"))
        return 9;
    else
        return 10;
    }
}