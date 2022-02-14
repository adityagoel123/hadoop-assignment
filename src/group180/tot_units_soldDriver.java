package group180;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The main application class. 
 * 
 * @author Group 180
 */
public class tot_units_soldDriver 
{
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: hadoopex <input path> <output path> <Item Type> <Country> <Queue Name>");
            System.exit(-1);
        }
        
        // create a configuration
     		Configuration conf = new Configuration();
     		conf.set("item_type", args[2]);
     		conf.set("country", args[3]);
     		conf.set("mapred.textoutputformat.separatorText", ",");
        // Create the job specification object
        Job job = new Job(conf);
        job.setJarByClass(tot_units_soldDriver.class);
        job.setJobName("Average Unit Price");

        // Setup input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the Mapper and Reducer classes
        job.setMapperClass(tot_units_soldMapper.class);
        job.setReducerClass(tot_units_soldReducer.class);
        //job.getConfiguration().set("mapreduce.job.queuename", args[4]);
        
        // Specify the type of output keys and values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        // Wait for the job to finish before terminating
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}