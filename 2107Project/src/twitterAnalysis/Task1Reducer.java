package twitterAnalysis;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
*    Reducer handles the shuffling and the post-processing of <k1,v1> pairs passed from Task1Mapper.
*	
*	 Author: Cheryl Tan
*/

public class Task1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	//function is to sort the total count according to the key
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, 
		Reducer<Text,IntWritable,Text,IntWritable>.Context context) 
			throws IOException, InterruptedException{
				int count = 0; //start at 0
				for(IntWritable value: values){
					count += value.get(); //get values
				}

				//write <key, value> pair to context and send to HDFS
				context.write(new Text(key.toString() + "\t"), new IntWritable(count));
				
				
			}
	
	
}
