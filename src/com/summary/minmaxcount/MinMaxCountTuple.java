package com.summary.minmaxcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class MinMaxCountTuple implements Writable {
	private Date min = new Date();
	private Date max = new Date();
	private long count = 0;
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	
	public Date getMin() {
		return min;
	}

	public void setMin(Date min) {
		this.min = min;
	}

	public Date getMax() {
		return max;
	}

	public void setMax(Date max) {
		this.max = max;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		min = new Date(in.readLong());
		max = new Date(in.readLong());
		count = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(min.getTime());
		out.writeLong(max.getTime());
		out.writeLong(count);
	}
	public String toString()
	{
		return frmt.format(min)+"\t"+frmt.format(max)+"\t"+count;
	}
}
