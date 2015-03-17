package com.inoutput.random;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class RandomInputFormat extends InputFormat<Text, NullWritable> {
	public static final String NUM_MAP_TASKS="random.generator.map.tasks";
	public static final String NUM_RECORDS_PER_TASK="random.generator.num.records.per.map.task";
	public static final String RANDOM_WORD_LIST = "random.generator.random.word.file";
	@Override
	public RecordReader<Text, NullWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		RandomRecordReader rr = new RandomRecordReader();
		rr.initialize(split, context);
		return rr;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		int numSplits = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);
		ArrayList<InputSplit> splits=new ArrayList<InputSplit>();
		
		for(int i=0;i<numSplits;i++)
		{
			splits.add(new FakeInputSplit());
		}
		
		return splits;
	}
	public static void setNumMapTask(Job job,int i)
	{
		job.getConfiguration().setInt(NUM_MAP_TASKS, i);
	}
	public static void setNumRecordPerTask(Job job,int i)
	{
		job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, i);
	}
	public static void setRandomWordList(Job job,Path file) throws URISyntaxException{
		DistributedCache.addCacheFile(file.toUri(), job.getConfiguration());
	}
}
