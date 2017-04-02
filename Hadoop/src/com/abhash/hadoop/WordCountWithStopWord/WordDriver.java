package com.abhash.hadoop.WordCountWithStopWord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Abhash
 * @category Hadoop
 * 
 * Setting the stop words in the Configration object
 * And retreiving the same from Mapper and emitting those words from
 * context object
 * 
 **/
public class WordDriver {
	
	public static void main(String args[]) throws Exception{
	
	Configuration conf = new Configuration();
	
	conf.set("stp","is,the,a,an");
	
	//conf.s
	Job job = Job.getInstance(conf, "Flight Max Delay");
	
	job.setJarByClass(WordDriver.class);
	job.setMapperClass(WordMapper.class);
	job.setReducerClass(WordReducer.class);
	job.setCombinerClass(WordReducer.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
	
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
//	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	
	}
}
