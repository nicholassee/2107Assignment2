//Created Chong Hiu Fung
package twitterAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class task4DMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	Text shape= new Text();
	IntWritable one=new IntWritable(1);
	@Override
	protected void map(LongWritable key,Text value,Mapper<LongWritable,Text,Text,IntWritable>.Context context)
		throws IOException,InterruptedException{
		String[] parts = value.toString().split(",");
		String airline_sentiment = parts[14];
		String airline = parts[16];
		if(airline!=null && airline_sentiment!=null){
			if(airline_sentiment.equals("positive")){
				context.write(new Text(airline),one);
			}
		}
	}

}
