package com.abhash.hadoop.flight.customInput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MinMaxCalculator implements Writable{
	
	int minDelay;
	int maxDelay;
	@Override
	public void readFields(DataInput in) throws IOException {
		minDelay=in.readInt();
		maxDelay=in.readInt();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(minDelay);
		out.writeInt(maxDelay);
		
	}
	public int getMinDelay() {
		return minDelay;
	}
	public void setMinDelay(int minDelay) {
		this.minDelay = minDelay;
	}
	public int getMaxDelay() {
		return maxDelay;
	}
	@Override
	public String toString() {
		return "MinMaxCalculator [minDelay=" + minDelay + ", maxDelay=" + maxDelay + "]";
	}
	public void setMaxDelay(int maxDelay) {
		this.maxDelay = maxDelay;
	}

}
