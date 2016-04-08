//Created by Chong Hiu Fung
package twitterAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//mapper class to create new key value pair from airline and airline sentiment
public class task4DMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	Text shape= new Text();
	IntWritable one=new IntWritable(1);
	@Override
	protected void map(LongWritable key,Text value,Mapper<LongWritable,Text,Text,IntWritable>.Context context)
		throws IOException,InterruptedException{
		//read one row and split by column
		String[] parts = value.toString().split(",");
		//Retrieve from column 14 airline sentiment and column 16 airline name
		String airline_sentiment = parts[14];
		String airline = parts[16];
		//check if airline and airline sentiment is no empty
		if(airline!=null && airline_sentiment!=null){
			//check if airline sentiment value is positive
			if(airline_sentiment.equals("positive")){
				//if airline is positive create new key value pair that consist of airline and sentiment with value as one
				context.write(new Text(airline),one);
			}
		}
	}

}
