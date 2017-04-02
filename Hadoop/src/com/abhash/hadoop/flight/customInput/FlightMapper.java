package com.abhash.hadoop.flight.customInput;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<LongWritable,Text,Text,MinMaxCalculator>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MinMaxCalculator>.Context context)
			throws IOException, InterruptedException {

		
		 MinMaxCalculator minmax = new MinMaxCalculator();
		String [] recordLine =value.toString().split(","); 
		
		if((recordLine[17].equals("IND") || recordLine[18].equals("IND")) && recordLine[23].equals("1"))
		{
			String flightNo=recordLine[9];
			int arrivalDelay=Integer.parseInt(recordLine[15]);
			if(arrivalDelay>0){
				minmax.setMaxDelay(arrivalDelay);
				minmax.setMinDelay(arrivalDelay);
				context.write(new Text(flightNo),minmax);
			}
		}
	
	}


	
	

}
