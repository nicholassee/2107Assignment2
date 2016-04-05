package twitterAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BonusTaskTwoMapper extends Mapper<Text, Text, Text, Text>{
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {	
		
		String[] parts = value.toString().split(",");
		String ip = parts[13];
		String tweet = parts[21];
		String user = parts[18];
		String sentiment = parts[14];
		if(isValidIP(ip)){
			context.write(new Text(ip), new Text(user+"\t"+sentiment+"\t"+tweet));
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