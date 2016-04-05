package twitterAnalysis;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class Task1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, 
		Reducer<Text,IntWritable,Text,IntWritable>.Context context) 
			throws IOException, InterruptedException{
				int count = 0;
				for(IntWritable value: values){
					count += value.get();
				}
				context.write(new Text(key.toString() + "\t"), new IntWritable(count));
				
				
			}
	
	
}
