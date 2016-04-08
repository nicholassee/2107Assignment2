/*
*Author : Benjamin Kuah
*/

package twitterAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BonusTaskTwoMapper extends Mapper<Text, Text, Text, Text>{
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {	
		
		//split the string and initializes the respective variables
		String[] parts = value.toString().split(",");
		String ip = parts[13];
		String tweet = parts[21];
		String user = parts[18];
		String sentiment = parts[14];
		
		//verifys if the ip address is valid , if it is , write to context the key of IP , and the Value of user , sentiment , tweet seperated by tabs
		if(isValidIP(ip)){
			context.write(new Text(ip), new Text(user+"\t"+sentiment+"\t"+tweet));
		}
	}
	/*
	*method check if a ip address is valid , to be considered valid , the parts returned must be 4 when split by " . "
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