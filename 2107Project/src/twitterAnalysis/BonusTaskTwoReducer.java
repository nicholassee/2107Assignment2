package twitterAnalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class BonusTaskTwoReducer extends Reducer<Text, Text, Text, Text>{
	static List<TempBonusTaskTwo> tempList = new LinkedList<>();
	/*
	 * Sorted by keyword IP
	 * Checks if tweets within is negative , if it is , add to a list, and add the user name and the number of times the same tweet has been tweeted
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		List<UniqueTweet> uniqueTweetList = new ArrayList<UniqueTweet>();//contains a unique tweetlist
		List<String> uniqueTweets = new ArrayList<String>();//for comparison purposes , 1 to 1 mapping with uniqueTweetList
		for(Text t: values){
			String[] parts = t.toString().split("\\\t");
			String userID = parts[0].trim();
			String sentiment = parts[1].trim();
			String tweet = parts[2].trim();
			if(sentiment.equals("negative"))
			{
				if(uniqueTweets.contains(tweet))
				{
					int index = uniqueTweets.indexOf(tweet);
					UniqueTweet temp = uniqueTweetList.get(index);
					temp.addUser(userID);
				}
				else
				{
					uniqueTweets.add(tweet);
					UniqueTweet temp = new UniqueTweet(tweet);
					temp.addUser(userID);
					uniqueTweetList.add(temp);
				}
			}
			
		}
		
		TempBonusTaskTwo temp = new TempBonusTaskTwo(key.toString(),uniqueTweetList);
		tempList.add(temp);
	}
	/*
	 * Write to the document the respective results. since we only want the situation which there are potential malicious users 
	 * Same Ip Address , Different Users , Same Tweet Message
	 * If there are at least 2 such senario , write to the document the IP Address, Tweet , Number of unique users , Username(number of same Tweet)
	 */
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException{
		Collections.sort(tempList,TempBonusTaskTwo.Comparators.USERCOUNT);
		for (int i = 0; i< tempList.size(); i++)
		{
			String key = tempList.get(i).key;
			List<UniqueTweet> uniqueTweetList = tempList.get(i).tweetsList;
			
			for (int j = 0; j<uniqueTweetList.size();j++)
			{
				UniqueTweet temp = uniqueTweetList.get(j);
				
				if(temp.noOfUniqueUsers>1)
				{
					String Results = "(uniqueUsers:"+temp.noOfUniqueUsers+")\t"+temp.tweet+"\t";
					
					List<String> users = temp.users;
					List<Integer> count = temp.count;
					
					for (int k = 0; k<users.size();k++)
					{
						Results += users.get(k)+"("+ count.get(k) +")\t";
					}
					context.write(new Text(tempList.get(i).key), new Text(Results));
				}
			}
			
		}
	}
}