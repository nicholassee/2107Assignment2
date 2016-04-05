package twitterAnalysis;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class task3DReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	IntWritable totalIW=new IntWritable();

	@Override
	protected void reduce(Text key,Iterable<IntWritable> values,Reducer<Text,IntWritable,Text,IntWritable>.Context context)
			throws IOException,InterruptedException{
		int total = 0;
		for(IntWritable value:values){
			total+=value.get();
		}
		context.write(key,new IntWritable(total));
	}

}
