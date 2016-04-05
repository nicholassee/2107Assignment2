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
		//read the cache file for country code and name
		@Override
		protected void setup(Mapper<LongWritable,Text,Text,IntWritable>.Context context)
						throws IOException,InterruptedException{
				BufferedReader br = new BufferedReader(new FileReader("ISO-3166-alpha3.tsv"));
				String line= null;
				while(true){
					line = br.readLine();
					if(line!=null){
						String parts[]=line.split("\t");
						countryCodes.put(parts[0],parts[1]);
					}else{
						break;
					}
				}
				br.close();
		}
		@Override
		protected void map(LongWritable key,Text value,Mapper<LongWritable,Text,Text,IntWritable>.Context context)
			throws IOException,InterruptedException{
			String[] parts = value.toString().split(",");
			String negtivereason = parts[15];
			String airline = parts[16];
			String countryCode = parts[10];
			//compare the three data to form a key 
			if(airline!=null && negtivereason!=null&&countryCode!=null){

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
