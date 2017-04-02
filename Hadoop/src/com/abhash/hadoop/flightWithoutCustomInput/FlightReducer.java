package com.abhash.hadoop.flightWithoutCustomInput;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightReducer extends Reducer<Text, LongWritable, Text,LongWritable>{
	
	private LongWritable MaxDelay;
	private Text _key;

	protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		MaxDelay = new LongWritable(0);
		_key = new Text("");
		
	}
	
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> value,
			Reducer<Text, LongWritable, Text, LongWritable>.Context arg2) throws IOException, InterruptedException {
		
		
		
		while(value.iterator().hasNext()){
			
			long temp=value.iterator().next().get();
			if(temp>MaxDelay.get()){
				MaxDelay=new LongWritable(temp);
				_key=key; 
			}
		}
		
	
	}

	protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		context.write(_key,MaxDelay);
		
		
	}

	
	
	

}
