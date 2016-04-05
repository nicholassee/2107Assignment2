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
		if (val.split(",").length >24)
		{
			val = val.replace(",", " , ");//ensures all commas can be read
			val = val.replace("\"\"", ""); // removes double ' "" '
			String [] before = val.split("\"");
			for( int i = 1; i<before.length-1 ; i+=2)
			{
				before[i] = before[i].replace(",", "");
				before[i] = before[i].replace("\n", "");
			}
			String results = "";
			for(int i = 0;i<before.length;i++)
			{
				results+=before[i];
			}
			int need = 27-results.split(",").length ;
			
			for (int i = 0; i<need; i++)
			{
				results+= " ,";
			}
			context.write(value, new Text(results));
		}
	}
}
