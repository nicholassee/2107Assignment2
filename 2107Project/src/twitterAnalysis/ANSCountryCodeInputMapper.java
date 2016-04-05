package twitterAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ANSCountryCodeInputMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] parts = value.toString().split("\t");
		String countryCode = parts[0];
		String countryName = parts[1];
		context.write(new Text(countryCode), new Text("countryName\t" + countryName));
	}
}
