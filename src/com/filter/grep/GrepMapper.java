package com.filter.grep;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {
	private String mapRegex = null;
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(value.toString().matches(mapRegex))
		{
			context.write(NullWritable.get(),value);
		}
	}

	@Override
	protected void setup(
			Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//mapRegex = context.getConfiguration().get("mapregex");
		mapRegex = "[0-9]+";
	}

}
