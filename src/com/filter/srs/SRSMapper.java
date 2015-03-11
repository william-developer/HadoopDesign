package com.filter.srs;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SRSMapper extends Mapper<Object, Text, NullWritable, Text> {
	private Random rands = new Random();
	private Double percentage;
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(rands.nextDouble()< percentage)
			context.write(NullWritable.get(), value);
	}

	@Override
	protected void setup(
			Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String strPercentage = context.getConfiguration().get("filter_percentage");
		percentage = Double.parseDouble(strPercentage);
	}

}
