package com.inoutput.random;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class RandomRecordReader extends RecordReader<Text, NullWritable> {
	public static final String NUM_MAP_TASKS="random.generator.map.tasks";
	public static final String NUM_RECORDS_PER_TASK="random.generator.num.records.per.map.task";
	public static final String RANDOM_WORD_LIST = "random.generator.random.word.file";
	private int numRecordsToCreate = 0;
	private int createRecords = 0;
	private Text key = new Text();
	private NullWritable value = NullWritable.get();
	private Random rndm = new Random();
	private ArrayList<String> randomWords = new ArrayList<String>();
	private SimpleDateFormat frmt= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return (float)createRecords/(float)numRecordsToCreate;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.numRecordsToCreate = context.getConfiguration().getInt(NUM_RECORDS_PER_TASK, -1);
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		BufferedReader rdr = new BufferedReader(new FileReader(files[0].toString()));
		String line;
		while((line=rdr.readLine())!=null)
		{
			randomWords.add(line);
		}
		rdr.close();
//		randomWords.add("system");
//		randomWords.add("print");    
//		randomWords.add("apple");    
//		randomWords.add("hello");    
//		randomWords.add("world");    
//		randomWords.add("hadoop");   
//		randomWords.add("ha");       
//		randomWords.add("hb");       
//		randomWords.add("jingdong"); 
//		randomWords.add("yamaxun");  
//		randomWords.add("taobao");   
//		randomWords.add("abc");      
//		randomWords.add("cba");      
//		randomWords.add("nba");      
//		randomWords.add("tom");      
//		randomWords.add("cat");      
//		randomWords.add("apache");   
//		randomWords.add("pig");      
//		randomWords.add("dog");      
//		randomWords.add("bag");      
//		randomWords.add("cup");      
//		randomWords.add("computer"); 
//		randomWords.add("mouse");    
//		randomWords.add("keybord");  
//		randomWords.add("henan");    
//		randomWords.add("hebei");    
//		randomWords.add("tianjin");  
//		randomWords.add("dalian");   
//		randomWords.add("zhengzhou");
//		randomWords.add("lanzhou");  
//		randomWords.add("gansu");    
//		randomWords.add("qingdao");  
//		randomWords.add("beijing");  
//		randomWords.add("chongqing");

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(createRecords<numRecordsToCreate)
		{
			int score = Math.abs(rndm.nextInt())%15000;
			int rowId = Math.abs(rndm.nextInt())%1000000000;
			int postId = Math.abs(rndm.nextInt())%100000000;
			int userId = Math.abs(rndm.nextInt())%1000000;
			String createDate = frmt.format(Math.abs(rndm.nextLong()));
			String text  =getRandomText();
			String randomRecord = rowId+"|"+postId+"|"+userId+"|"+score+"|"+text+"|"+createDate;
			key.set(randomRecord);
			++createRecords;
			return true;
		}
		else
		{
			return false;
		}
	}
	private String getRandomText(){
		StringBuilder bldr = new StringBuilder();
		int numWords = Math.abs(rndm.nextInt())%30+1;
		for(int i=0;i<numWords;i++)
		{
			bldr.append(randomWords.get(Math.abs(rndm.nextInt())%randomWords.size())+"");
		}
		return bldr.toString();
	}
}
