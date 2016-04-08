/*
*Author : Benjamin Kuah
*/

package twitterAnalysis;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
public class TempBonusTaskTwo implements Comparable<TempBonusTaskTwo>{
	String key;
	List<UniqueTweet> tweetsList = new ArrayList<UniqueTweet>();
	public TempBonusTaskTwo(String key, List<UniqueTweet> tweetsList)
	{
		this.key = key;
		this.tweetsList = tweetsList;
	}
	
	//iterator comparator so that the list knows to compare the tweetsize instead.
	@Override
	public int compareTo(TempBonusTaskTwo o)
	{
		if(tweetsList.size()> o.tweetsList.size())
		{
			return 1;
		}
		return 0;
	}
	
	public static class Comparators{
		
		public static Comparator<TempBonusTaskTwo> TWEETLISTCOUNT = new Comparator<TempBonusTaskTwo>()
				{
					@Override
					public int compare(TempBonusTaskTwo o1, TempBonusTaskTwo o2)
					{
						if (o1.tweetsList.size() > o2.tweetsList.size()) return -1;
				        if (o1.tweetsList.size() < o2.tweetsList.size()) return 1;
				        return 0;
					}
				};
	}
}
