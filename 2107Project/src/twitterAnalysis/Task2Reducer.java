package twitterAnalysis;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class Task2Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private Map countMap = new HashMap<>();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, 
		Reducer<Text,IntWritable,Text,IntWritable>.Context context) 
			throws IOException, InterruptedException{
				int count = 0;
				for(IntWritable value: values){
					count += value.get();
				}
				//context.write(key, new IntWritable(count));
				
				countMap.put(new Text(key), new IntWritable(count));
			}
	
	protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
		Map<Text, IntWritable> sortedMap = sortByValue(countMap);
		
		int counter = 0;
		for (Text key: sortedMap.keySet()){
			context.write(new Text(key.toString() + "\t"), sortedMap.get(key));
			if (counter++ == 0){
				break;
			}
		}
	}
	
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map){
		List<Map.Entry<K,V>> list= new LinkedList<>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>(){
			@Override
			public int compare(Map.Entry<K,V> o1, Map.Entry<K, V> o2){
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});
			
		Map<K, V> result= new LinkedHashMap<>();
		for(Map.Entry<K,V> entry: list){
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
}
