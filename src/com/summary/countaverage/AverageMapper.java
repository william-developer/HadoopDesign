package com.summary.countaverage;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {
	private IntWritable outHour = new IntWritable();
	private CountAverageTuple outCountAverage = new CountAverageTuple();
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, IntWritable, CountAverageTuple>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] line = value.toString().split("\\|");
		String strDate = line[3];
		String text = line[2];
		Date createDate =null;
		try {
			createDate = frmt.parse(strDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		outHour.set(createDate.getHours());
		outCountAverage.setCount(1);
		outCountAverage.setAverage(text.length());
		
		context.write(outHour, outCountAverage);
	}

}
