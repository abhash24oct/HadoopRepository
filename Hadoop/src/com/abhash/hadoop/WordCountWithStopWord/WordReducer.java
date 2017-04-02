package com.abhash.hadoop.WordCountWithStopWord;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer  extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> value,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		int sum=0;
//		int values = (int) ((LongWritable) value).get(); 
		while(value.iterator().hasNext()){
			sum +=value.iterator().next().get();
		}
		
		context.write(key,new LongWritable(sum));
		
	}
	
	

}
