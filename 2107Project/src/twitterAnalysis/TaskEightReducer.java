package twitterAnalysis;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Contributor: Leonard Yeo (14SIC082T)
 * */
public class TaskEightReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	/*
	 * function to iterate through values that is associated with the key (Airline name + Sentiment word)
	 * */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		
		int count = 0;
		
		for(IntWritable t: values){
			count += t.get();
		}
		
		String parts[] = key.toString().split("\t");
		
		String str = String.format("%s\t%s\t", parts[0],parts[1]); //formats the string
		
		context.write(new Text(str), new IntWritable(count));
	}
	
}
