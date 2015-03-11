package com.summary.medianstddev2;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianStdDevCombiner extends
		Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable> {

	@Override
	protected void reduce(
			IntWritable key,
			Iterable<SortedMapWritable> values,
			Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		SortedMapWritable outValue = new SortedMapWritable();
		for(SortedMapWritable v:values)
		{
			for(Entry<WritableComparable,Writable> entry:v.entrySet())
			{	
				LongWritable count = (LongWritable)outValue.get(entry.getKey());
				if(count!=null)
				{
					outValue.put(entry.getKey(), new LongWritable(((LongWritable)entry.getValue()).get()));
				}
				else
				{
					outValue.put(entry.getKey(), new LongWritable(((LongWritable)entry.getValue()).get()));
				}
			}
		}
	}

}
