package twitterAnalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaskFiveReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		ArrayList<Double> airlineTrustPoints = new ArrayList<Double>();
		for (Text value: values) {
			String[] parts = value.toString().split("\t");
			airlineTrustPoints.add(Double.parseDouble(parts[1].toString()));
		}
		Collections.sort(airlineTrustPoints);
		if(airlineTrustPoints.size() % 2 == 1){
			double median = airlineTrustPoints.get((airlineTrustPoints.size() + 1)/2-1);
			context.write(new Text(key.toString()), new DoubleWritable(median));
		}
		else{
			double lowerIndex = airlineTrustPoints.get(airlineTrustPoints.size()/2-1);
			double upperIndex = airlineTrustPoints.get(airlineTrustPoints.size()/2);
			double median = (lowerIndex + upperIndex) / 2.0;
			context.write(new Text(key.toString()), new DoubleWritable(median));
		}		
	}
}