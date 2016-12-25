package com.abhash.weather;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		
		String str= value.toString();
		
		if(!(str.length()==0)){
			
			String date=str.substring(6, 14);
			
			float tempMax=Float.parseFloat(str.substring(39,45).trim());
			float tempMin=Float.parseFloat(str.substring(47,53).trim());

			if(tempMax>35.02){
				
				context.write(new Text("Hot Day on "+date), new Text(String.valueOf(tempMax)));
				
			}
			
			if(tempMin<10){
				context.write(new Text("Cold day on "+date),new Text(String.valueOf(tempMin)));
			}
			
		}
		
			
		}
		
	}
	

