package twitterAnalysis;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class BonusTaskReducer extends Reducer<Text, Text, Text, Text>{
	static List<Temp> tempList = new LinkedList<>();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		int delayCount = 0;
		int totalCount = 0;
		for(Text t: values){
			if (t.toString().equals("delay"))
			{
				delayCount++;
			}
			totalCount++;
		}
		Temp temp = new Temp(key.toString(),delayCount,totalCount);
		tempList.add(temp);
	}
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException{
		Collections.sort(tempList,Temp.Comparators.probability);
		for (int i = 0; i< tempList.size(); i++)
		{
			Text tempText = new Text();
			tempText.set(tempList.get(i).key);
			double probability = (double)tempList.get(i).probability;
			String str = String.format("Prob Delay %f", probability);
			if(probability != 0)
			{
				context.write(tempText,new Text(str));
			}
			else
			{
				i = tempList.size();//if its 0 , there is no point displaying it any more , so just end
			}
			
		}
	}
}