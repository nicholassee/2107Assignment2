/*
*Author : Benjamin Kuah
*/

package twitterAnalysis;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Task7Reducer extends Reducer<Text, Text, Text, Text> {
	static int total = 0;//holds the value of the number of unique ip addresses
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
				//init the count variable to count the number of tweets per key/ip
		int count = 0;
		total++;//add the number of unique ip addresses
		for(Text t: values){
			count++;//increment the number of tweets
		}
		
		String str = String.format("Tweeted %d times", count);//formats a string for display purposes
		context.write(key, new Text(str));//write to the context the respective key and the number of times the tweet has been tweeted
	}
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException{
		
		//since this is run at the end of the reducer , write to the context the total number of unique ip that has tweeted.
		String str = String.format("%d\t", total);
		context.write(new Text("Total Unique IP:"), new Text(str));
	}
}
