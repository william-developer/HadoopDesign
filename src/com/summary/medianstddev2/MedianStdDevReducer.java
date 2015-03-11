package com.summary.medianstddev2;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianStdDevReducer extends
		Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdDevTuple> {
	private MedianStdDevTuple result = new MedianStdDevTuple();
	private TreeMap<Integer,Long> commentLengthCounts = new TreeMap<Integer,Long>();
	@Override
	protected void reduce(
			IntWritable key,
			Iterable<SortedMapWritable> values,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		float sum = 0;
		long totalComments = 0;
		commentLengthCounts.clear();
		result.setStdDve(0);
		result.setMedian(0);
		
		for(SortedMapWritable v:values)
		{
			for(Entry<WritableComparable,Writable> entry:v.entrySet())
			{
				int length = ((IntWritable)entry.getKey()).get();
				long count = ((LongWritable)entry.getValue()).get();
				totalComments+=count;
				sum+=length*count;
				
				Long storeCount = commentLengthCounts.get(length);
				if(storeCount==null)
					commentLengthCounts.put(length, count);
				else
					commentLengthCounts.put(length, storeCount+count);
				
			}
		}
		long medianIndex = totalComments / 2L;
		long previousComments = 0;
		long comments=0;
		int preKey =0;
		
		for(Entry<Integer,Long> entry: commentLengthCounts.entrySet())
		{
			comments = previousComments +entry.getValue();
			if(previousComments<=medianIndex&&medianIndex<comments)
			{
				if(totalComments%2==0&&previousComments==medianIndex)
				{
					result.setMedian((float)(entry.getKey()+preKey)/2.0f);
				}else
				{
					result.setMedian(entry.getKey());
				}
				break;
			}
			previousComments = comments;
			preKey = entry.getKey();
		}
		
		float mean = sum/totalComments;
		float sumOfSquares = 0.0f;
		for(Entry<Integer,Long> entry: commentLengthCounts.entrySet())
		{
			sumOfSquares +=(entry.getKey()-mean)*(entry.getKey()-mean)*entry.getValue();
			
		}
		result.setStdDve((float)Math.sqrt(sumOfSquares)/(totalComments-1));
		context.write(key, result);
	}

}
