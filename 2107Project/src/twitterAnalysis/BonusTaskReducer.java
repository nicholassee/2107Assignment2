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
	static List<Temp> tempList = new LinkedList<>();//temporary list for storing all temporary objects
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
				// init delay count and total count to 0 
		int delayCount = 0;// delay count only adds up the tweet with the mention of delay
		int totalCount = 0;//total count is to count the number of values there are , which is Delay + non delay
		for(Text t: values){
			if (t.toString().equals("delay"))//checks that the tweet has a delay
			{
				delayCount++;
			}
			totalCount++;
		}
		Temp temp = new Temp(key.toString(),delayCount,totalCount);//create a temporary object , this object consist of a key and some special variables which gives the ability to count probability.
		tempList.add(temp);//add the object to the temporary list for use in the cleanup to display
	}
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException{
		Collections.sort(tempList,Temp.Comparators.probability);//sort the collection according to its probability that was calculated
		//goes through the tempList 
		for (int i = 0; i< tempList.size(); i++)
		{
			//obtain the respective information
			Text tempKey = new Text();
			tempKey.set(tempList.get(i).key);
			double probability = (double)tempList.get(i).probability;
			String str = String.format("Prob Delay %f", probability);
			if(probability != 0)// if the probability is not 0 , write to context the key and the text probabilty that was calculated
			{
				context.write(tempKey,new Text(str));
			}
			else//since it is sorted according to probability , if the probability is 0 , there is no point displaying , 
			{
				i = tempList.size();//if its 0 , there is no point displaying it any more , so just end
			}
			
		}
	}
}