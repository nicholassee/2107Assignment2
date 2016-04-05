package twitterAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BonusTaskTwoValidationMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {	
		String val = value.toString();
		
		//checks that the split is above 24 , which is most case safe to say its a proper row
		if (val.split(",").length >24)
		{
			val = val.replace(",", " , ");//ensures all commas can be read
			val = val.replace("\"\"", ""); // removes double ' "" '
			
			//this part checks for any situation with a value like " Hi, Testing" where there is a , in between and remove that , 
			String [] before = val.split("\"");
			for( int i = 1; i<before.length-1 ; i+=2)
			{
				before[i] = before[i].replace(",", "");
			}
			String results = "";//create a result string and init to "" to be used for the final string
			
			
			//Append all the strings in the before to the results
			for(int i = 0;i<before.length;i++)
			{
				results+=before[i];
			}
			
			//verify the number of additional " , " needed to make the column valid. and add it to the string.
			int need = 27-results.split(",").length ;
			for (int i = 0; i<need; i++)
			{
				results+= " ,";
			}
			
			//Write to the context the results
			context.write(value, new Text(results));
		}
	}
}
