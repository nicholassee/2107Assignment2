package twitterAnalysis;

import java.util.ArrayList;
import java.util.List;

public class UniqueTweet {
	String tweet;
	int noOfUniqueUsers = 0;;
	List<String> users = new ArrayList<String>();
	List<Integer> count = new ArrayList<Integer>();
	public UniqueTweet(String tweet){
		this.tweet = tweet;
	}
	public void addUser(String user)
	{
		if(users.contains(user))
		{
			int index = users.indexOf(user);
			int counter = count.get(index);
			counter++;
			count.set(index,counter);
		}
		else
		{
			users.add(user);
			count.add(1);
			noOfUniqueUsers++;
		}
	}
}
