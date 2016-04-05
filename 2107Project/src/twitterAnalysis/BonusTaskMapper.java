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
	Hashtable<String, String> countryCodes = new Hashtable<>();
	String[] dayList = {"Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"};
	String[] timeCategoryList = {"12AM to 6AM","6AM to 12PM", "12PM to 6PM","6pm to 12AM"} ;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// We will put the ISO-3166-alpha3.tsv to Distributed Cache in the driver class
		// so we can access to it here locally by its name
		BufferedReader br = new BufferedReader(new FileReader("ISO-3166-alpha3.tsv"));
		String line = null;
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
		String[] parts = value.toString().split(",");
		String airline = parts[16];
		String countryCode = parts[10];
		String tweet = parts[21];
		String tweetCreated = parts[23];
		
	    DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:MM"); 
	    Date date;
	    try {
	    	date = df.parse(tweetCreated);
	        String newDateString = df.format(date);
	        int day = date.getDay();
	        int hour = date.getHours();
	        int TimeCategory = hour/6;
	        
	        if(!airline.equals("") && !tweet.equals("") && !countryCode.equals(""))
			{
				String countryName = countryCodes.get(countryCode);
				if (countryName != null) {
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