package com.abhash.hadoop.flight.customInput;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightReducer extends Reducer<Text, MinMaxCalculator, Text,MinMaxCalculator>{

	MinMaxCalculator result = new MinMaxCalculator();
	
	@Override
	protected void reduce(Text key, Iterable<MinMaxCalculator> value,
			Reducer<Text, MinMaxCalculator, Text, MinMaxCalculator>.Context context)
					throws IOException, InterruptedException {
		
		Integer mindelay=0;
		Integer maxdelay=0;
		result.setMaxDelay(-1);
		result.setMinDelay(-1);
		for(MinMaxCalculator temp : value){
			mindelay=temp.getMinDelay();
			maxdelay=temp.getMaxDelay();
			if(temp.getMinDelay()==-1 || mindelay.compareTo(result.getMinDelay())<0){
				result.setMinDelay(mindelay);
			}
			if(temp.getMaxDelay()==-1 || maxdelay.compareTo(result.getMaxDelay())<0){
				result.setMinDelay(maxdelay);
			}
		}
		
		context.write(key,result);
		
	}


	protected void cleanup(Reducer<Text, MinMaxCalculator, Text, MinMaxCalculator>.Context context)
			throws IOException, InterruptedException {
		
	
	}

	
	protected void setup(Reducer<Text, MinMaxCalculator, Text, MinMaxCalculator>.Context context)
			throws IOException, InterruptedException {
		
		
	}

	
		
	
	
	

}
