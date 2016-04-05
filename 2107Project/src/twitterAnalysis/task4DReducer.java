package twitterAnalysis;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class task4DReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	IntWritable totalIW=new IntWritable();
	private TreeMap<Integer,Text>sortedMap= new TreeMap<Integer,Text>();		
	@Override
	protected void reduce(Text key,Iterable<IntWritable> values,Reducer<Text,IntWritable,Text,IntWritable>.Context context)throws
	IOException,InterruptedException{
		int total = 0;
		for(IntWritable value:values){
			total+=value.get();
		}
		sortedMap.put(total,new Text(key));
	}
	@Override
	protected void cleanup(Context context)
		throws IOException,InterruptedException{
		int count=0;
		for(int t:sortedMap.descendingMap().keySet())
		{
			if(count<3){
				totalIW.set(t);
				context.write(sortedMap.get(t),totalIW);
			}
			count++;
		}
	
	}


}
