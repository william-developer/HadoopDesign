package com.dataorganize.shuffling;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnonymizeMapper extends Mapper<Object, Text, IntWritable, Text> {
	private IntWritable outKey = new IntWritable();
	private Random rndm = new Random();
	private Text outValue = new Text();
	@Override
	protected void map(Object key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] line = value.toString().split("\\|");
		String strDate = line[3];
		String text = line[2];
		if(line.length>0)
		{
			StringBuilder bldr = new StringBuilder();
			bldr.append(line[1]);
			bldr.append(text);
			bldr.append(strDate.substring(strDate.indexOf('T')));
			outKey.set(rndm.nextInt());
			outValue.set(bldr.toString());
			context.write(outKey, outValue);
		}
		
	}

}
