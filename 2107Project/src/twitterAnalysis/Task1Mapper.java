package twitterAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Hashtable<String, String>countryCodes = new Hashtable<>();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
		throws IOException, InterruptedException{
			BufferedReader br = new BufferedReader(new FileReader("ISO-3166-alpha3.tsv"));
			
			String line = null;
			while(true){
				line = br.readLine();
				if(line != null){
					String parts[] = line.split("\t");
					countryCodes.put(parts[0], parts[1]);
				}
				else{
					break;
				}
			}
			br.close();
		}
		
	@Override
	protected void map(LongWritable key, Text value, 
		Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException{
				String[] parts = value.toString().split(","); //split by column
				//String countryCode = parts[10]; //in column 10
				String sentiment = parts[14];  // in column 14
				String reason = parts[15];
			
					if(sentiment != null && reason!= null){ //if both column not empty
						
						//if negative and have country code
						if(sentiment.equals("negative") && reason != null){
							context.write(new Text(reason), new IntWritable(1));
						}
					}
				//}
				
			}
}
