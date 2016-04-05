package twitterAnalysis;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ANSTwitterInputMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if(isValid(value.toString())){
			String[] parts = value.toString().split(",");
			String trustPoint = parts[8];
			String airline = parts[16];
			if (airline != null && NumberUtils.isNumber(trustPoint)) {
				context.write(new Text(airline), new Text("trustPoint\t" + trustPoint));
			}
		}
	}
	private boolean isValid(String line){
		String[] parts = line.split(",");
		if(parts.length == 27){
			return true;
		}else{
			return false;
		}
	}
}
