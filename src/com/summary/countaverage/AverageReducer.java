package com.summary.countaverage;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {
	private CountAverageTuple result = new CountAverageTuple();

	@Override
	protected void reduce(
			IntWritable key,
			Iterable<CountAverageTuple> values,
			Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		float sum =0;
		float count =0;
		for(CountAverageTuple val:values)
		{
			sum+=val.getCount()*val.getAverage();
			count+=val.getCount();
		}
		result.setAverage(sum/count);
		result.setCount(count);
		context.write(key, result);
	}

}
