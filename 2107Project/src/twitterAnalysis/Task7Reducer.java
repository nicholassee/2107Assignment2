package twitterAnalysis;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Task7Reducer extends Reducer<Text, Text, Text, Text> {
	static int total = 0;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		int count = 0;
		total++;
		for(Text t: values){
			count++;
		}
		
		String str = String.format("Tweeted %d times", count);
		context.write(key, new Text(str));
	}
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException{
		String str = String.format("%d\t", total);
		context.write(new Text("Total Unique IP:"), new Text(str));
	}
}
