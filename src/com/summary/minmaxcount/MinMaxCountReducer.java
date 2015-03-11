package com.summary.minmaxcount;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxCountReducer extends
		Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
	private MinMaxCountTuple result = new MinMaxCountTuple();
	@Override
	protected void reduce(Text key, Iterable<MinMaxCountTuple> values,
			Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		result.setMax(null);
		result.setMin(null);
		result.setCount(0);
		
		int sum = 0;
		for(MinMaxCountTuple val:values)
		{
			if(result.getMin()==null||val.getMin().compareTo(result.getMin())<0)
			{
				result.setMin(val.getMin());
			}
			if(result.getMax() ==null||val.getMax().compareTo(result.getMax())>0)
			{
				result.setMax(val.getMax());
			}
			sum+=val.getCount();
				
		}
		result.setCount(sum);
		context.write(key,result);
		
	}

}
