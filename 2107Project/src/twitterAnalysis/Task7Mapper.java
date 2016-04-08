/*
*Author : Benjamin Kuah
*/

package twitterAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task7Mapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {	
		//validates if the row is valid
		if(isValid(value.toString())){
			//splits string and inits the respective variables
			String[] parts = value.toString().split(",");
			String ip = parts[13];
			String tweet = parts[21];
			if(isValidIP(ip)){//validates that the ip address is valid. and write key as IP and tweet as tweet. Note : any value can be put here as we have not used tweet in the next mapper
				context.write(new Text(ip), new Text(tweet));
		
			}
		}
	}
	/*
	*Row is valid assuming it has 27 parts when split by ","
	*/
	private boolean isValid(String line){
		String[] parts = line.split(",");
		if (parts.length==27){
			return true;
		}else{
			return false;
		}
	}
	/*
	*ip address is valid assuming it has 4 parts when split by "."
	*/
	private boolean isValidIP(String ip){
		String[] parts = ip.split("\\.");
		if (parts.length==4){
			return true;
		}else{
			return false;
		}
	}
}
