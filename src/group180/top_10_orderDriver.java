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
public class top_10_orderDriver 
{
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: hadoopex <input path> <output path> <Year> <Queue Name>");
            System.exit(-1);
        }
        
        // create a configuration
     		Configuration conf = new Configuration();
     		conf.set("year", args[2]);
     		conf.set("mapred.textoutputformat.separatorText", ",");
        // Create the job specification object
        Job job = new Job(conf);
        job.setJarByClass(top_10_orderDriver.class);
        job.setJobName("Average Unit Price");

        // Setup input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the Mapper and Reducer classes
        job.setMapperClass(top_10_orderMapper.class);
        job.setReducerClass(top_10_orderReducer.class);
        //job.getConfiguration().set("mapreduce.job.queuename", args[3]);
        // Specify the type of output keys and values
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
  
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        // Wait for the job to finish before terminating
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}