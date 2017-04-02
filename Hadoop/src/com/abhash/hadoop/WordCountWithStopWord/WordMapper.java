package com.abhash.hadoop.WordCountWithStopWord;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	private HashSet<String> stpWords=null;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String stopWord = conf.get("stp");
		stpWords = new HashSet<String>();
		String[] stopWords =stopWord.split(",");
		
		for(String temp : stopWords){
			stpWords.add(temp);
		}
	}
	

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String words= value.toString();
		String wordToWrite=null;
		Text WordWritten = new Text();
			
			StringTokenizer stk = new StringTokenizer(words, ",");
			
			while(stk.hasMoreTokens()){
				wordToWrite=stk.nextToken();
				WordWritten.set(wordToWrite);
				if(stpWords.contains(wordToWrite))
					continue;
				context.write(WordWritten,new LongWritable(1));
				
				
				
			}
			
			
		}
		
	}

	
	
	


