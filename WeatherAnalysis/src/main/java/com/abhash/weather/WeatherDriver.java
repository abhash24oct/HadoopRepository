package com.abhash.weather;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

public class WeatherDriver {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
       Configuration conf = new Configuration();
		
		//Initializing the job with the default configuration of the cluster		
       Job job = Job.getInstance(conf, "word count");
		
		//Assigning the driver class name
		job.setJarByClass(WeatherDriver.class);

		//Key type coming out of mapper
		job.setMapOutputKeyClass(Text.class);
		
		//value type coming out of mapper
		job.setMapOutputValueClass(Text.class);

		//Defining the mapper class name
		job.setMapperClass(WeatherMapper.class);
		
		//Defining the reducer class name
		job.setReducerClass(WeatherReducere.class);

		//Defining input Format class which is responsible to parse the dataset into a key value pair
		job.setInputFormatClass(TextInputFormat.class);
		
		//Defining output Format class which is responsible to parse the dataset into a key value pair
		job.setOutputFormatClass(TextOutputFormat.class);

		//setting the second argument as a path in a path variable
		Path OutputPath = new Path(args[1]);

		//Configuring the input path from the filesystem into the job
		FileInputFormat.addInputPath(job, new Path(args[0]));

		//Configuring the output path from the filesystem into the job
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//deleting the context path automatically from hdfs so that we don't have delete it explicitly
		OutputPath.getFileSystem(conf).delete(OutputPath);

		//exiting the job only if the flag value becomes false
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

