package com.abhash.hadoop.flightWithoutCustomInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlightDriver {
	
	public static void main(String [] args) throws Exception{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Flight Max Delay");
//	job.getConfiguration().addResource(new Path());
//	job.getConfiguration().addResource(new Path()); 
	job.setJarByClass(FlightDriver.class);
	job.setMapperClass(FlightMapper.class);
	//job.setNumReduceTasks(1);
	job.setReducerClass(FlightReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
	}

}
