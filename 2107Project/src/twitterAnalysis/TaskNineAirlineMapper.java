package twitterAnalysis;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Contributor: Leonard Yeo (14SIC082T)
 * */
public class TaskNineAirlineMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * The purpose of this mapper is to remove duplicated tweets
	 * For example:
	 * American Airlines, @asd helloworld!
	 * American Airlines, @asd helloworld!
	 * American Airlines, @asd helloworld!
	 * American Airlines, @asd helloworld!
	 * 
	 * Essentially, duplicates will be removed and 
	 * returns American Airlines, @asd helloworld!
	 * */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		if(isValid(value.toString())){
			
			String record = value.toString();
			String[] parts = record.split(",");
			String airline = parts[16];
			String tweets = parts[21];
			String trust = parts[8];
			
			if(airline != null && tweets != null && trust != null){
				
				String concat = airline+"\t"+tweets;
				String concatWithTrust = concat+"\t"+trust;
				
				context.write(new Text(tweets), new Text(concatWithTrust));
				 
			}
		}
	}
	
	private boolean isValid(String line){
		String[] parts = line.split(",");
		if(parts.length == 27){
			return true;
		}else{
			return false;
		}
	}
	
}
