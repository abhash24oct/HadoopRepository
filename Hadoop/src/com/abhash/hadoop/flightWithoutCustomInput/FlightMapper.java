package com.abhash.hadoop.flightWithoutCustomInput;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String [] recordLine =value.toString().split(","); 
		
		if((recordLine[16].equals("IND")) && recordLine[21].equals("0"))
		{
			String flightNo=recordLine[9];
			try{
				int arrivalDelay=Integer.parseInt(recordLine[14]);
				if(arrivalDelay>0){
					context.write(new Text(flightNo),new LongWritable(arrivalDelay));
				}
			}
			catch(NumberFormatException n){
				return;
			}
		}
	}
	
	

}
