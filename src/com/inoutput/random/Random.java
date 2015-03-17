package com.inoutput.random;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;
/**
 * 
 * @ClassName Random.java
 * @Description TODO
 * @author william
 * @date 2015Äê3ÔÂ17ÈÕ
 * @command hadoop jar random.jar 2 11 hdfs://localhost:9000/user/input/cache /user/output/1
 */
public class Random {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		int numMapTasks = Integer.parseInt(args[0]);
		int numRecordsPerTask = Integer.parseInt(args[1]);
		Path wordList = new Path(args[2]);
		Path outputDir = new Path(args[3]);
		
		Job job = new Job(conf,"Random");
		job.setJarByClass(Random.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(RandomInputFormat.class);
		RandomInputFormat.setNumMapTask(job, numMapTasks);
		RandomInputFormat.setNumRecordPerTask(job, numRecordsPerTask);
		
		RandomInputFormat.setRandomWordList(job, wordList);
		
		TextOutputFormat.setOutputPath(job, outputDir);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
