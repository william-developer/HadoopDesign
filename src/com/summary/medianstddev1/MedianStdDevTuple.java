package com.summary.medianstddev1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MedianStdDevTuple implements Writable {
	private float stdDve;
	private float median;
	
	public float getStdDve() {
		return stdDve;
	}

	public void setStdDve(float stdDve) {
		this.stdDve = stdDve;
	}

	public float getMedian() {
		return median;
	}

	public void setMedian(float median) {
		this.median = median;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		stdDve = in.readFloat();
		median = in.readFloat(); 
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeFloat(stdDve);
		out.writeFloat(median);
	}
	public String toString()
	{
		return stdDve+"\t"+median;
	}
}
