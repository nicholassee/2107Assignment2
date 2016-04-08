//Created by Chong Hiu Fung
package twitterAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class task3DMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	//hash table for the country code
		Hashtable<String,String> countryCodes= new Hashtable<>();
		IntWritable one=new IntWritable(1);
		//This function is run before starting the actual map
		@Override
		protected void setup(Mapper<LongWritable,Text,Text,IntWritable>.Context context)
						throws IOException,InterruptedException{
				//read the cache file for country code and name
				BufferedReader br = new BufferedReader(new FileReader("ISO-3166-alpha3.tsv"));
				String line= null;
				
				while(true){
					//read one line of the document
					line = br.readLine();
					//while line is not empty keep reading
					if(line!=null){
						//split the word by the tab spacing
						String parts[]=line.split("\t");
						//add to hastable country code and country name
						countryCodes.put(parts[0],parts[1]);
					}
					//break at the end of the file
					else{
						break;
					}
				}
				br.close();
		}
		@Override
		protected void map(LongWritable key,Text value,Mapper<LongWritable,Text,Text,IntWritable>.Context context)
			throws IOException,InterruptedException{
			//read one row and split by column
			String[] parts = value.toString().split(",");
			//get negative reason from column 15
			String negtivereason = parts[15];
			//get airline from column 16
			String airline = parts[16];
			//get country code from column 10
			String countryCode = parts[10];
			//check if each column is not null
			if(airline!=null && negtivereason!=null&&countryCode!=null){
				//if the retrieve 
				if(negtivereason.equals("CSProblem")||negtivereason.equals("badflight")){		
					//get the country name from the hash table
					String countryName= countryCodes.get(countryCode);
					if(countryName != null && !countryName.isEmpty()){
						//form the new key value
						String str = countryName+" "+airline+" "+negtivereason;
						//write for reducer to use
						context.write(new Text(str),one);
					}
				}
			}
		}

}
