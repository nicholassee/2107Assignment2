package twitterAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task7Mapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {	
		
		if(isValid(value.toString())){
			
			String[] parts = value.toString().split(",");
			String ip = parts[13];
			String tweet = parts[21];
			if(isValidIP(ip)){
				context.write(new Text(ip), new Text(tweet));
		
			}
		}
	}
	private boolean isValid(String line){
		String[] parts = line.split(",");
		if (parts.length==27){
			return true;
		}else{
			return false;
		}
	}
	private boolean isValidIP(String ip){
		String[] parts = ip.split("\\.");
		if (parts.length==4){
			return true;
		}else{
			return false;
		}
	}
}
