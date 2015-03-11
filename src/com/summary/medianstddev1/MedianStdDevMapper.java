package com.summary.medianstddev1;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianStdDevMapper extends
		Mapper<Object, Text, IntWritable, IntWritable> {
	private IntWritable outHour = new IntWritable();
	private IntWritable outCommentLength = new IntWritable();
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	@Override
	protected void map(Object key, Text value, Context context)
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
		outCommentLength.set(text.length());
		context.write(outHour, outCommentLength);
	}

}
