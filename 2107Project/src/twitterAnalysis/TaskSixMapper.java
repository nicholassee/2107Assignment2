package twitterAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaskSixMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,
			Text, IntWritable>.Context context)throws IOException, InterruptedException{
		String[] parts = value.toString().split(",");
		if(parts[14].equals("negative") && parts[21].toLowerCase().contains("delay")){
			context.write(new Text("delay"), new IntWritable(1));
		}
	}
}
