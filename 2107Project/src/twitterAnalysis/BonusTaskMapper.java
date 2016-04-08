/*
*Author : Benjamin Kuah
*/

package twitterAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BonusTaskMapper extends Mapper<LongWritable, Text, Text, Text>{
	Hashtable<String, String> countryCodes = new Hashtable<>();//hashmap for the purpose of Mapper side join.
	String[] dayList = {"Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"};//list of the days  from sunday to monday for use with date.getDay()
	String[] timeCategoryList = {"12AM to 6AM","6AM to 12PM", "12PM to 6PM","6pm to 12AM"} ;//category that is created for time.
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// We will put the ISO-3166-alpha3.tsv to Distributed Cache in the driver class
		// so we can access to it here locally by its name
		BufferedReader br = new BufferedReader(new FileReader("ISO-3166-alpha3.tsv"));
		String line = null;
		
		//init the countryCode list so that we can perform a mapper side join
		while (true) {
			line = br.readLine();
			if (line != null) {
				String parts[] = line.split("\t");
				countryCodes.put(parts[0], parts[1]);
			} else {
				break;// finished reading
			}
		}
		br.close();
	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {	
		// perform splitting and getting of the respective variables
		String[] parts = value.toString().split(",");
		String airline = parts[16];
		String countryCode = parts[10];
		String tweet = parts[21];
		String tweetCreated = parts[23];
		
		//format the date and attempt to resolve it , throws exception if there is an error
	    DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:MM"); 
	    Date date;
	    try {
	    	date = df.parse(tweetCreated);
	        String newDateString = df.format(date);
	        int day = date.getDay();
	        int hour = date.getHours();
	        int TimeCategory = hour/6;
	        
			//verifys that the columns are not empty
	        if(!airline.equals("") && !tweet.equals("") && !countryCode.equals(""))
			{
				//perform mapper side join to replace country code with country
				String countryName = countryCodes.get(countryCode);
				if (countryName != null) {
					//verify that the tweet contains a delay , if not put none as the key value
					//Key is using a combination of airline , country name , day of the week , and time category.
					if(tweet.toLowerCase().contains("delay"))
					{
						context.write(new Text(airline+"\t"+countryName+"\t"+dayList[day]+"\t"+timeCategoryList[TimeCategory]), new Text("delay"));
					}
					else
					{
						context.write(new Text(airline+"\t"+countryName+"\t"+dayList[day]+"\t"+timeCategoryList[TimeCategory]), new Text("none"));
					}
					
				}
			}
	    } catch (Exception e) {
	        //e.printStackTrace();
	    }
	}
}