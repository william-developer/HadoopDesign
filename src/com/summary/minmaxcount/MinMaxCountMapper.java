package com.summary.minmaxcount;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {
	private Text outUserId = new Text();
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, MinMaxCountTuple>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] line = value.toString().split("\\|");
		String strDate = line[3];
		String userId = line[4];
		Date createDate =null;
		try {
			createDate = frmt.parse(strDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		outTuple.setMax(createDate);
		outTuple.setMin(createDate);
		
		outTuple.setCount(1);
		outUserId.set(userId);
		
		context.write(outUserId, outTuple);
	}
}
