//Created by Chong Hiu Fung
package twitterAnalysis;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class task4DReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	IntWritable totalIW=new IntWritable();
	//Create new Treemap to store the value 
	private TreeMap<Integer,Text>sortedMap= new TreeMap<Integer,Text>();		
	@Override
	protected void reduce(Text key,Iterable<IntWritable> values,Reducer<Text,IntWritable,Text,IntWritable>.Context context)throws
	IOException,InterruptedException{
		int total = 0;
		//for every same key/value pair increment by one
		for(IntWritable value:values){
			total+=value.get();
		}
		//put the pair into the treemap with total been the key and the key as the value
		sortedMap.put(total,new Text(key));
	}
	//this is run when key/value pairs are all present within the reducer method
	@Override
	protected void cleanup(Context context)
		throws IOException,InterruptedException{
		int count=0;
		//sort the treemap by the key value which the total count of possible value
		//run through the list
		for(int t:sortedMap.descendingMap().keySet())
		{
			// count is below 3 context write them
			if(count<3){
				//set the key from treemap as intwritable
				totalIW.set(t);
				//context write the new key/value price which consist of the treemap value as key and treemap key as the value.
				context.write(sortedMap.get(t),totalIW);
			}
			count++;
		}
	
	}


}
