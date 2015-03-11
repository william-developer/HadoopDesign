package com.summary.medianstddev1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianStdDevReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple> {
	private MedianStdDevTuple result = new MedianStdDevTuple();
	private ArrayList<Float> commentLengths = new ArrayList<Float>();
	@Override
	protected void reduce(
			IntWritable key,
			Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		float sum = 0;
		float count = 0;
		commentLengths.clear();
		result.setStdDve(0);
		
		for(IntWritable val:values)
		{
			commentLengths.add((float)val.get());
			sum +=val.get();
			++count;
		}
		Collections.sort(commentLengths);
		
		if(count%2==0)
		{
			result.setMedian(commentLengths.get((int)count/2+1));
		}
		else
		{
			result.setMedian(commentLengths.get((int)count/2));
		}
		float mean  = sum/count;
		
		float sumOfSquares=0;
		for(Float f:commentLengths)
		{
			sumOfSquares +=(f-mean)*(f-mean);
		}
		result.setStdDve((float)Math.sqrt(sumOfSquares)/(count-1));
		context.write(key, result);
	}

}
