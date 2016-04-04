package twitterAnalysis;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

/*
 * Contributor: Leonard Yeo (14SIC082T)
 * */
public class TaskEightSentimentMapper extends Mapper<Text, Text, Text, IntWritable>{
	
	SentiWordNet sentiwordnet;
	String[] dictionary = {"neutral", "very happy", "happy", "somewhat happy", "somewhat sad", "sad", "very sad"};
	
	
	@Override
	protected void setup(Mapper<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException{
		URI[] conf = context.getCacheFiles(); //get the URI from the file that was created from TaskEight.java 
		sentiwordnet = new SentiWordNet(conf[0]); //creates a dictionary of words with their values
	}
	
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		
			String record = value.toString();
			String[] parts = record.split("\t");
			String airline = parts[0];
			String tweets = parts[1];
			double num = 0;
			
			if(airline != null && !airline.equals("airline") 
					&& tweets != null && !tweets.equals("text") ){
				
				 num = SentiWord(tweets); //get the value that is associated with the tweet
				 String senti = getSenti(num); //find out which sentimental word is associated with the value
				 
				 
				 if(senti.equals(dictionary[0])){
					 
					 context.write(new Text(airline+"\t"+dictionary[0]), new IntWritable(1));
					 
				 }else if(senti.equals(dictionary[1])){
					 
					 context.write(new Text(airline+"\t"+dictionary[1]), new IntWritable(1));
					 
				 }else if(senti.equals(dictionary[2])){
					 
					 context.write(new Text(airline+"\t"+dictionary[2]), new IntWritable(1));
					 
				 }else if(senti.equals(dictionary[3])){
					 
					 context.write(new Text(airline+"\t"+dictionary[3]), new IntWritable(1));
					 
				 }else if(senti.equals(dictionary[4])){
					 
					 context.write(new Text(airline+"\t"+dictionary[4]), new IntWritable(1));
					 
				 }else if(senti.equals(dictionary[5])){
					 
					 context.write(new Text(airline+"\t"+dictionary[5]), new IntWritable(1));
					 
				 }else{
					 
					 context.write(new Text(airline+"\t"+dictionary[6]), new IntWritable(1));
					 
				 }
				 
			}
	}
	
	
	/*
	 * function to get the sentimental word through matching the value with a set of value range
	 * */
	public String getSenti(double value){
		
		String sent = "neutral";
		
		if(value >= 0.75){ //if very happy
			sent = "very happy";
		}else if(value > 0.25 && value <= 0.5){ //if happy
			sent = "happy";
		}else if(value > 0 && value >= 0.25){ //if somewhat happy
			sent = "somewhat happy";
		}else if(value < 0 && value >= -0.25){ //if somewhat sad
			sent = "somewhat sad";
		}else if(value < -0.25 && value >= -0.5){ //if sad
			sent = "sad";
		}else if(value <= -0.75){ //very sad
			sent = "very sad";
		}
		
		return sent;
	}
	
	/*
	 * function to return the value of the tweet
	 * by spliting the tweet into words
	 * those individual words are then matched with the dictionary provided by SentiWordNet.java
	 * those values will then be incremented into the counter
	 * */
	public double SentiWord(String word) throws IOException{
		
		String[] wordSpilt = word.split(" ");
		
		double counter = 0.0;
		
		for(int i=0; i<wordSpilt.length; i++){
			counter += sentiwordnet.extract(wordSpilt[i].replaceAll("[^a-zA-Z]", "").toLowerCase(), "a");
		}
		
		return counter;
	}
	
}
