/*
*Author : Benjamin Kuah
*/

package twitterAnalysis;

import java.util.Comparator;
public class Temp implements Comparable<Temp>{
	String key;
	int delayCount;
	int totalCount;
	double probability;
	public Temp(String key, int delayCount,int totalCount)
	{
		this.key = key;
		this.delayCount = delayCount;
		this.totalCount = totalCount;
		this.probability = (double)delayCount/totalCount;//probability count is noted by doing a delay count divided by the total count which tells us how many delays against total.
	}
	
	
	//allows the comparison of probability instead.
	@Override
	public int compareTo(Temp o)
	{
		if(probability> o.probability)
		{
			return 1;
		}
		return 0;
	}
	
	public static class Comparators{
		
		public static Comparator<Temp> probability = new Comparator<Temp>()
				{
					@Override
					public int compare(Temp o1, Temp o2)
					{
						if (o1.probability > o2.probability) return -1;
				        if (o1.probability < o2.probability) return 1;
				        return 0;
					}
				};
	}
}
