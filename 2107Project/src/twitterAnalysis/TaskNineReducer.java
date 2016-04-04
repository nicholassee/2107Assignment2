package twitterAnalysis;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Contributor: Leonard Yeo (14SIC082T)
 * */
public class TaskNineReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	IntWritable totalIW=new IntWritable();
	private TreeMap<Integer,Text>sortedMap= new TreeMap<Integer,Text>();
	
	/*
	 * function to iterate through values that is associated with the key (Airline name + Sentiment word)
	 * */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		
		int total=0;
		for(IntWritable value:values){
			total +=value.get();
		}
		
		String[] parts = key.toString().split("\t");
		
		sortedMap.put(total,new Text(parts[0]));
		
	}
	
	/*
	 * Cleanup to sort the keys by descending order
	 * write into context the value based on the sorted key
	 * */
	@Override
	protected void cleanup(Context context)
		throws IOException,InterruptedException{
		
		for(int t:sortedMap.descendingMap().keySet())
		{
			totalIW.set(t);
			String text = String.format("%s\t%s", sortedMap.get(t)," score is: ");
			context.write(new Text(text),totalIW);
		}
	
	}
	
}