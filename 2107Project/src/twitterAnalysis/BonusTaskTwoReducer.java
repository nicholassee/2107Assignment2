/*
*Author : Benjamin Kuah
*/

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
			// Iterates through , split stirng by tabs and inits the respective columns and trim the text to remove excess white spaces
			String[] parts = t.toString().split("\\\t");
			String userID = parts[0].trim();
			String sentiment = parts[1].trim();
			String tweet = parts[2].trim();
			
			//checks that the sentiment is negative.
			if(sentiment.equals("negative"))
			{
				//check that the tweet has been added before
				if(uniqueTweets.contains(tweet))
				{
					//assuming the tweet has been created before , update the respective tweet by adding the user into the uniqueTweet object
					int index = uniqueTweets.indexOf(tweet);
					UniqueTweet temp = uniqueTweetList.get(index);
					temp.addUser(userID);
				}
				else
				{
					//assuming this is a first of its tweet , add to the comparison list and create a unique tweet object and add to the object list.
					uniqueTweets.add(tweet);
					UniqueTweet temp = new UniqueTweet(tweet);
					temp.addUser(userID);
					uniqueTweetList.add(temp);
				}
			}
			
		}
		//create a temporary object to hold both the key and the list , this temporary object is capable of knowing how many users tweeted the same tweet.
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
		
		//sort the collection by the number of unique tweets inside each Key
		Collections.sort(tempList,TempBonusTaskTwo.Comparators.TWEETLISTCOUNT);
		for (int i = 0; i< tempList.size(); i++)
		{
			//get the unique tweet list for iteration
			String key = tempList.get(i).key;
			List<UniqueTweet> uniqueTweetList = tempList.get(i).tweetsList;
			
			for (int j = 0; j<uniqueTweetList.size();j++)
			{
				
				UniqueTweet temp = uniqueTweetList.get(j);
				//checks if there is at least 2 user for the same unique tweet 
				if(temp.noOfUniqueUsers>1)
				{
					//assuming there is at least 2 users for this ip address and exact same tweet, add number of unique users and the actual tweet to the results list.
					String Results = "(uniqueUsers:"+temp.noOfUniqueUsers+")\t"+temp.tweet+"\t";
					
					
					//get the information for displaying the list of users and the number of times they tweeted the same tweet.
					List<String> users = temp.users;
					List<Integer> count = temp.count;
					
					//add the users to the results string
					for (int k = 0; k<users.size();k++)
					{
						Results += users.get(k)+"("+ count.get(k) +")\t";
					}
					
					//write to the context the IP address , and the results
					context.write(new Text(tempList.get(i).key), new Text(Results));
				}
			}
			
		}
	}
}