package twitterAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
*    Task 2 asks to get the country with the highest number of complain.
*    Assumption for this task is to get one country out of all the complains.
*	 Author: Cheryl Tan
*/
public class Task2Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Hashtable<String, String>countryCodes = new Hashtable<>();
	
	//this function creates the setup to read the country code and set as <key,value> pair
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
		throws IOException, InterruptedException{
			BufferedReader br = new BufferedReader(new FileReader("ISO-3166-alpha3.tsv"));
			
			String line = null; //set line to null
			while(true){ //when condition is true
				line = br.readLine(); // set line to read from buffer
				if(line != null){ //if each line is not nulll (when it has value)
					String parts[] = line.split("\t"); //split array when there is a tab
					countryCodes.put(parts[0], parts[1]); // set put(key, value)
				}
				else{
					break;
				}
			}
			br.close();
		}
		
	//this function filter the condition to send context of country name to the reducer
	// output are partitioned into the local partitions and shuffled to reducer
	@Override
	protected void map(LongWritable key, Text value, 
		Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException{
				String[] parts = value.toString().split(","); //split by column
				String countryCode = parts[10]; //in column 10
				String sentiment = parts[14];  // in column 14
				String reason = parts[15];
				String airline = parts[16];
				//context.write(new Text("Airline Country Total_count"), new IntWritable());
				//if (countryCode !=null){
					if(countryCode !=null && sentiment != null && reason!= null){ //if both column not empty
						String countryName = countryCodes.get(countryCode);
						//if negative and have country code
						if(sentiment.equals("negative") && countryName != null){
							context.write(new Text(countryName), new IntWritable(1));
						}
					}
				//}
				
			}
}
