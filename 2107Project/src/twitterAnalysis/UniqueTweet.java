package twitterAnalysis;

import java.util.ArrayList;
import java.util.List;

public class UniqueTweet {
	String tweet;
	int noOfUniqueUsers = 0;
	List<String> users = new ArrayList<String>();
	List<Integer> count = new ArrayList<Integer>();
	public UniqueTweet(String tweet){
		this.tweet = tweet;
	}
	/*
	*adding a user checks if the user list already contains that user , if it contains the user , it simply increases the counter for that users
	*else the user will be added and the counter for that user will be initialized
	*/
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
