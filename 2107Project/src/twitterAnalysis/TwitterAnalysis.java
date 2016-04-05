package twitterAnalysis;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.geom.AffineTransform;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.EmptyBorder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import twitterAnalysis.TwitterAnalysis;
import twitterAnalysis.TwitterAnalysis.Bar;

public class TwitterAnalysis extends JFrame{
	
	private JPanel contentPane;
	 private String[] tasks = { "Task 1", "Task 2", "Task 3",
		      "Task 4", "Task 5", "Task 6", "Task 7", "Task 8", "Task 9", "Bonus 1", "Bonus 2" };
	 private JComboBox comboBox = new JComboBox();
	 private JButton runTaskButton = new JButton("Run task");
	 private int count = 0;
	 private String[] outputPaths = {"hdfs://localhost:9000/user/phamvanvung/airline/taskOneOutput/part-r-00000", "hdfs://localhost:9000/user/phamvanvung/airline/taskTwoOutput/part-r-00000", 
			 "hdfs://localhost:9000/user/phamvanvung/airline/taskThreeOutput/part-r-00000", "hdfs://localhost:9000/user/phamvanvung/airline/taskFourOutput/part-r-00000",
			 "hdfs://localhost:9000/user/phamvanvung/airline/taskFiveOutput/part-r-00000", "hdfs://localhost:9000/user/phamvanvung/airline/taskSixOutput/part-r-00000",
			 "hdfs://localhost:9000/user/phamvanvung/airline/taskSevenOutput/part-r-00000", "hdfs://localhost:9000/user/phamvanvung/airline/taskEightOutput/part-r-00000",
			 "hdfs://localhost:9000/user/phamvanvung/airline/taskNineOutput/part-r-00000", "hdfs://localhost:9000/user/phamvanvung/airline/bonusTaskOutput/part-r-00000",
			 "hdfs://localhost:9000/user/phamvanvung/airline/bonusTaskTwoOutput/part-r-00000"};
//	 private String[] taskDescription = {"How many distinct negative reasons", "People from which country complained the most", "Number of badflight or CSProblem for each country",
//			 "Top three countries with most positive tweets", "Median trust point for each airline", "How many delayed flights", "How many unique IP", "How many angry/happy/etc tweets for each airline",
//			 "top five airline based on true sentiments", "Forecasted delay time and day for each airline", "Highly suspected troll"};
	
	public static void main(String[] args) throws Exception {
		// path setup
		Path twitterInput1 = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/Airline-Full-Non-Ag-DFE-Sentiment_20150225.csv");
		Path twitterInput2 = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/Airline-Full-Non-Ag-DFE-Sentiment_20150226.csv");
		Path twitterInput3 = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/Airline-Full-Non-Ag-DFE-Sentiment_20150227.csv");
		Path twitterInput4 = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/Airline-Full-Non-Ag-DFE-Sentiment_20150228.csv");
		Path twitterInput5 = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/Airline-Full-Non-Ag-DFE-Sentiment_20150301.csv");
		Path twitterInput6 = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/Airline-Full-Non-Ag-DFE-Sentiment_20150302.csv");
		Path twitterInput7 = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/Airline-Full-Non-Ag-DFE-Sentiment_20150303.csv");
		Path twitterInput8 = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/Airline-Full-Non-Ag-DFE-Sentiment_20150304.csv");
		Path twitterInput9 = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/Airline-Full-Non-Ag-DFE-Sentiment_20150305.csv");
		Path twitterInput10 = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/Airline-Full-Non-Ag-DFE-Sentiment_20150306.csv");
		Path countryCodeInput = new Path("hdfs://localhost:9000/user/phamvanvung/airline/ISO-3166-alpha3.tsv");
		Path bonusTaskTwoInput = new Path("hdfs://localhost:9000/user/phamvanvung/airline/bonusTaskTwoInput/Airline-Full-Non-Ag-DFE-Falsified.csv");
		
		// configuration for generic validation
		Configuration validationConf = new Configuration(false);
		//task 1
		Configuration taskOneConf = new Configuration();
		Job taskOneJob = Job.getInstance(taskOneConf,"TwitterAnalysis");
		taskOneJob.setJarByClass(TwitterAnalysis.class);
		Path taskOneOutPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/taskOneOutput");
		taskOneOutPath.getFileSystem(taskOneConf).delete(taskOneOutPath, true);
		taskOneJob.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/airline/ISO-3166-alpha3.tsv"));
		ChainMapper.addMapper(taskOneJob, ANSValidationMapper.class, LongWritable.class, 
				Text.class, LongWritable.class, Text.class, validationConf);
		Configuration ansConfTaskOne = new Configuration(false);
		ChainMapper.addMapper(taskOneJob, Task1Mapper.class, LongWritable.class, 
				Text.class, Text.class, IntWritable.class, ansConfTaskOne);
		taskOneJob.setMapperClass(ChainMapper.class);
		taskOneJob.setCombinerClass(Task1Reducer.class);
		taskOneJob.setReducerClass(Task1Reducer.class);
		taskOneJob.setOutputKeyClass(Text.class);
		taskOneJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(taskOneJob, twitterInput1);
		FileOutputFormat.setOutputPath(taskOneJob, taskOneOutPath);
		taskOneJob.waitForCompletion(true);
		
		//task 2
		Configuration taskTwoConf = new Configuration();
		Job taskTwoJob = Job.getInstance(taskTwoConf,"TwitterAnalysis");
		taskTwoJob.setJarByClass(TwitterAnalysis.class);
		Path taskTwoOutPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/taskTwoOutput");
		taskTwoOutPath.getFileSystem(taskTwoConf).delete(taskTwoOutPath, true);
		taskTwoJob.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/airline/ISO-3166-alpha3.tsv"));
		ChainMapper.addMapper(taskTwoJob, ANSValidationMapper.class, LongWritable.class, 
				Text.class, LongWritable.class, Text.class, validationConf);
		Configuration ansConfTaskTwo = new Configuration(false);
		ChainMapper.addMapper(taskTwoJob, Task2Mapper.class, LongWritable.class, 
				Text.class, Text.class, IntWritable.class, ansConfTaskTwo);
		taskTwoJob.setMapperClass(ChainMapper.class);
		taskTwoJob.setCombinerClass(Task2Reducer.class);
		taskTwoJob.setReducerClass(Task2Reducer.class);
		taskTwoJob.setOutputKeyClass(Text.class);
		taskTwoJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(taskTwoJob, twitterInput1);
		FileOutputFormat.setOutputPath(taskTwoJob, taskTwoOutPath);
		taskTwoJob.waitForCompletion(true);
		
		//task 3
		Configuration taskThreeConf = new Configuration();
		Job taskThreeJob = Job.getInstance(taskThreeConf,"TwitterAnalysis");
		taskThreeJob.setJarByClass(TwitterAnalysis.class);
		Path taskThreeOutPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/taskThreeOutput");
		taskThreeOutPath.getFileSystem(taskThreeConf).delete(taskThreeOutPath, true);
		//check if the data have no issue
		ChainMapper.addMapper(taskThreeJob,ANSValidationMapper.class,LongWritable.class,
				Text.class,LongWritable.class,Text.class,validationConf);
		Configuration ansConfTaskThree = new Configuration(false);
		//do the mapping part
		ChainMapper.addMapper(taskThreeJob,task3DMapper.class,LongWritable.class,Text.class,
				Text.class,IntWritable.class,ansConfTaskThree);
		//add the cahce file to be read
		taskThreeJob.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/airline/ISO-3166-alpha3.tsv"));
		taskThreeJob.setMapperClass(ChainMapper.class);	
		taskThreeJob.setCombinerClass(task3DReducer.class);
		taskThreeJob.setReducerClass(task3DReducer.class);
		taskThreeJob.setOutputKeyClass(Text.class);
		taskThreeJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(taskThreeJob,twitterInput1);
		FileOutputFormat.setOutputPath(taskThreeJob, taskThreeOutPath);
		taskThreeJob.waitForCompletion(true);
		
		//task 4
		Configuration taskFourConf = new Configuration();
		Job taskFourJob = Job.getInstance(taskFourConf, "TwitterAnalysis");
		taskFourJob.setJarByClass(TwitterAnalysis.class);
		Path taskFourOutPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/taskFourOutput");
		taskFourOutPath.getFileSystem(taskFourConf).delete(taskFourOutPath, true);
		ChainMapper.addMapper(taskFourJob,ANSValidationMapper.class,LongWritable.class,
				Text.class,LongWritable.class,Text.class,validationConf);
		Configuration ansConfTaskFour = new Configuration(false);
		ChainMapper.addMapper(taskFourJob,task4DMapper.class,LongWritable.class,Text.class,
				Text.class,IntWritable.class,ansConfTaskFour);
		taskFourJob.setMapperClass(ChainMapper.class);
		
		taskFourJob.setCombinerClass(task4DReducer.class);
		taskFourJob.setReducerClass(task4DReducer.class);
		
		taskFourJob.setOutputKeyClass(Text.class);
		taskFourJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(taskFourJob,twitterInput1);
		FileOutputFormat.setOutputPath(taskFourJob, taskFourOutPath);
		taskFourJob.waitForCompletion(true);
		
		//task 5
		Configuration taskFiveConf = new Configuration();
		Job taskFiveJob = Job.getInstance(taskFiveConf, "TwitterAnalysis");
		taskFiveJob.setJarByClass(TwitterAnalysis.class);
		taskFiveJob.setReducerClass(TaskFiveReducer.class);
		taskFiveJob.setOutputKeyClass(Text.class);
		taskFiveJob.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(taskFiveJob, twitterInput1, TextInputFormat.class, TaskFiveMapper.class);
		//MultipleInputs.addInputPath(taskFiveJob, twitterInput2, TextInputFormat.class, TaskFiveMapper.class);
		//MultipleInputs.addInputPath(taskFiveJob, twitterInput3, TextInputFormat.class, TaskFiveMapper.class);
		//MultipleInputs.addInputPath(taskFiveJob, twitterInput4, TextInputFormat.class, TaskFiveMapper.class);
		//MultipleInputs.addInputPath(taskFiveJob, twitterInput5, TextInputFormat.class, TaskFiveMapper.class);
		//MultipleInputs.addInputPath(taskFiveJob, twitterInput6, TextInputFormat.class, TaskFiveMapper.class);
		//MultipleInputs.addInputPath(taskFiveJob, twitterInput7, TextInputFormat.class, TaskFiveMapper.class);
		//MultipleInputs.addInputPath(taskFiveJob, twitterInput8, TextInputFormat.class, TaskFiveMapper.class);
		//MultipleInputs.addInputPath(taskFiveJob, twitterInput9, TextInputFormat.class, TaskFiveMapper.class);
		//MultipleInputs.addInputPath(taskFiveJob, twitterInput10, TextInputFormat.class, TaskFiveMapper.class);
		Path taskFiveoutPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/taskFiveOutput");
		taskFiveoutPath.getFileSystem(taskFiveConf).delete(taskFiveoutPath, true);
		FileOutputFormat.setOutputPath(taskFiveJob, taskFiveoutPath);
		taskFiveJob.waitForCompletion(true);
		
		//Task 6
		Configuration taskSixConf = new Configuration();
		Job taskSixJob = Job.getInstance(taskSixConf, "TwitterAnalysis");
		taskSixJob.setJarByClass(TwitterAnalysis.class);
		Path taskSixOutpath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/taskSixOutput");
		taskSixOutpath.getFileSystem(taskSixConf).delete(taskSixOutpath, true);
		//set to false to not use default configuration settings
		//Configuration validationConf = new Configuration(false);
		ChainMapper.addMapper(taskSixJob, ANSValidationMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, validationConf);
		
		Configuration ansConfTaskSix = new Configuration(false);
		ChainMapper.addMapper(taskSixJob, TaskSixMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class, ansConfTaskSix);
		
		taskSixJob.setMapperClass(ChainMapper.class);
		taskSixJob.setCombinerClass(TaskSixReducer.class);
		taskSixJob.setReducerClass(TaskSixReducer.class);
		
		taskSixJob.setOutputKeyClass(Text.class);
		taskSixJob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(taskSixJob, twitterInput1);
		FileOutputFormat.setOutputPath(taskSixJob, taskSixOutpath);
		
		taskSixJob.waitForCompletion(true);
		
		//System.exit(taskSixJob.waitForCompletion(true)?0:1);
		
		//task seven
		Configuration task7conf = new Configuration();
		Job task7job = Job.getInstance(task7conf,"TwitterAnalysis");
		task7job.setJarByClass(TwitterAnalysis.class);
		task7job.setMapperClass(Task7Mapper.class);
		task7job.setReducerClass(Task7Reducer.class);
		task7job.setOutputKeyClass(Text.class);
		task7job.setOutputValueClass(Text.class);
		Path task7Outpath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/taskSevenOutput");
		task7Outpath.getFileSystem(task7conf).delete(task7Outpath, true);
		FileInputFormat.addInputPath(task7job,twitterInput1);
		FileOutputFormat.setOutputPath(task7job,task7Outpath);
		task7job.waitForCompletion(true);
		
		// task 8
		Configuration taskEightconf = new Configuration();
		Job taskEightJob = Job.getInstance(taskEightconf,"TwitterAnalysis");
		taskEightJob.setJarByClass(TwitterAnalysis.class);
		Path taskEigthOutpath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/taskEightOutput");
		taskEigthOutpath.getFileSystem(taskEightconf).delete(taskEigthOutpath, true);
		taskEightJob.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/airline/Senti.txt"));
		Configuration airlineConfTaskEight = new Configuration(false);
		ChainMapper.addMapper(taskEightJob, TaskEightAirlineMapper.class, LongWritable.class, Text.class, Text.class, Text.class, airlineConfTaskEight);
		Configuration tweetsConfTaskEight = new Configuration(false);
		ChainMapper.addMapper(taskEightJob, TaskEightSentimentMapper.class, Text.class, Text.class, Text.class, IntWritable.class, tweetsConfTaskEight);
		taskEightJob.setMapperClass(ChainMapper.class);
		taskEightJob.setCombinerClass(TaskEightReducer.class);
		taskEightJob.setReducerClass(TaskEightReducer.class);
		taskEightJob.setOutputKeyClass(Text.class);
		taskEightJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(taskEightJob, twitterInput1);
		FileOutputFormat.setOutputPath(taskEightJob, taskEigthOutpath);
		taskEightJob.waitForCompletion(true);
		
		// task 9
		Configuration taskNineconf = new Configuration();
		Job taskNineJob = Job.getInstance(taskNineconf,"TwitterAnalysis");
		taskNineJob.setJarByClass(TwitterAnalysis.class);
		Path taskNineOutpath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/taskNineOutput");
		taskNineOutpath.getFileSystem(taskNineconf).delete(taskNineOutpath, true);
		taskNineJob.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/airline/Senti.txt"));
		Configuration airlineConfTaskNine = new Configuration(false);
		ChainMapper.addMapper(taskNineJob, TaskNineAirlineMapper.class, LongWritable.class, Text.class, Text.class, Text.class, airlineConfTaskNine);
		Configuration tweetsConfTaskNine = new Configuration(false);
		ChainMapper.addMapper(taskNineJob, TaskNineSentimentMapper.class, Text.class, Text.class, Text.class, IntWritable.class, tweetsConfTaskNine);
		
		taskNineJob.setMapperClass(ChainMapper.class);
		taskNineJob.setCombinerClass(TaskNineReducer.class);
		taskNineJob.setReducerClass(TaskNineReducer.class);
		
		taskNineJob.setOutputKeyClass(Text.class);
		taskNineJob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(taskNineJob, twitterInput1);
		FileOutputFormat.setOutputPath(taskNineJob, taskNineOutpath);
		
		taskNineJob.waitForCompletion(true);
		//System.exit((task7job.waitForCompletion(true))?0:1);
		
		// bonus task 1
		Configuration bonusTaskConf = new Configuration();
		Job bonusTaskJob = Job.getInstance(bonusTaskConf,"TwitterAnalysis");
		bonusTaskJob.setJarByClass(TwitterAnalysis.class);
		bonusTaskJob.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/airline/ISO-3166-alpha3.tsv"));
		ChainMapper.addMapper(bonusTaskJob, ANSValidationMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, validationConf);
		//Second mapper does logic , Checking for Airlines , Country and Time Category as a key with a Delay or None for the value
		Configuration ansConfBonus = new Configuration(false);
		ChainMapper.addMapper(bonusTaskJob, BonusTaskMapper.class, LongWritable.class, Text.class, Text.class, Text.class, ansConfBonus);		
		bonusTaskJob.setMapperClass(ChainMapper.class);
		bonusTaskJob.setReducerClass(BonusTaskReducer.class);
		
		bonusTaskJob.setOutputKeyClass(Text.class);
		bonusTaskJob.setOutputValueClass(Text.class);
		Path bonusTaskOutpath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/bonusTaskOutput");
		bonusTaskOutpath.getFileSystem(bonusTaskConf).delete(bonusTaskOutpath, true);
		FileInputFormat.addInputPath(bonusTaskJob,twitterInput1);
		FileOutputFormat.setOutputPath(bonusTaskJob,bonusTaskOutpath);
		bonusTaskJob.waitForCompletion(true);
		
		// bonus task 2
		Configuration bonusTaskTwoConf = new Configuration();
		Job bonusTaskTwoJob = Job.getInstance(bonusTaskTwoConf,"TwitterAnalysis");
		bonusTaskTwoJob.setJarByClass(TwitterAnalysis.class);
		bonusTaskTwoJob.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/airline/ISO-3166-alpha3.tsv"));
		//First mapepr does validation to ensure that the row valid
		Configuration validationConfBonusTaskTwo = new Configuration(false);
		ChainMapper.addMapper(bonusTaskTwoJob, BonusTaskTwoValidationMapper.class, LongWritable.class, Text.class, Text.class, Text.class, validationConfBonusTaskTwo);
		//Second mapper does logic , Checking for Airlines , Country and Time Category as a key with a Delay or None for the value
		Configuration ansConfBonusTaskTwo = new Configuration(false);
		ChainMapper.addMapper(bonusTaskTwoJob, BonusTaskTwoMapper.class, Text.class, Text.class, Text.class, Text.class, ansConfBonusTaskTwo);
		bonusTaskTwoJob.setMapperClass(ChainMapper.class);
		bonusTaskTwoJob.setReducerClass(BonusTaskTwoReducer.class);
		bonusTaskTwoJob.setOutputKeyClass(Text.class);
		bonusTaskTwoJob.setOutputValueClass(Text.class);
		Path bonusTaskTwoOutpath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/bonusTaskTwoOutput");
		bonusTaskTwoOutpath.getFileSystem(bonusTaskTwoConf).delete(bonusTaskTwoOutpath, true);
		FileInputFormat.addInputPath(bonusTaskTwoJob,bonusTaskTwoInput);
		FileOutputFormat.setOutputPath(bonusTaskTwoJob,bonusTaskTwoOutpath);
		bonusTaskTwoJob.waitForCompletion(true);
		
		// UI
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					TwitterAnalysis frame = new TwitterAnalysis();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	/**
	 * Create the frame.
	 */
	public TwitterAnalysis() {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 1050, 900);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);
		JPanel panel = new JPanel();
	    panel.setBounds(0, 455, 1000, 400);
		JTextArea textArea = new JTextArea();
		ArrayList<Color> colorArray = new ArrayList<Color>();
		colorArray.add(Color.RED);
		colorArray.add(Color.BLUE);
		colorArray.add(Color.GREEN);
		colorArray.add(Color.ORANGE);
		colorArray.add(Color.YELLOW);
		colorArray.add(Color.GRAY);
		colorArray.add(Color.BLACK);
		colorArray.add(Color.CYAN);
		colorArray.add(Color.PINK);
		colorArray.add(Color.MAGENTA);
		for (int i = 0; i < tasks.length; i++)
			comboBox.addItem(tasks[count++]);
		runTaskButton.setBounds(130, 69, 135, 25);
		runTaskButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if (count < tasks.length)
					comboBox.addItem(tasks[count++]);
				System.out.println("index: " + comboBox.getSelectedIndex());
				panel.removeAll();
			    panel.updateUI();
				try {
					Configuration uIConf = new Configuration();
					int selection = comboBox.getSelectedIndex();
					URI uri;
					switch(selection){
					case 0:
						uri = new URI(outputPaths[0]);
						break;
					case 1:
						uri = new URI(outputPaths[1]);
						break;
					case 2:
						uri = new URI(outputPaths[2]);
						break;
					case 3:
						uri = new URI(outputPaths[3]);
						break;
					case 4:
						uri = new URI(outputPaths[4]);
						break;
					case 5:
						uri = new URI(outputPaths[5]);
						break;
					case 6:
						uri = new URI(outputPaths[6]);
						break;
					case 7:
						uri = new URI(outputPaths[7]);
						break;
					case 8:
						uri = new URI(outputPaths[8]);
						break;
					case 9:
						uri = new URI(outputPaths[9]);
						break;
					case 10:
						uri = new URI(outputPaths[10]);
						break;
					default:
						uri = new URI(outputPaths[0]);
						break;
					}
					FileSystem fSystem = FileSystem.get(uri, uIConf);
					BufferedReader br = new BufferedReader( new InputStreamReader (fSystem.open(
							new Path(uri))));
					String line= null;
					textArea.setText("");
					if(selection == 3){
						int tempCounter = 0;
						int roundOffFigure;
						ArrayList<Bar> values = new ArrayList<Bar>();
						while(true){
							line = br.readLine();
							if(line!=null){
								String parts[]=line.split("\t");
								System.out.println("part 0:" + parts[0] + " parts 1:" + parts[1]);
								String str = textArea.getText();
								textArea.setText(str + line + "\n");
								if(tempCounter>=colorArray.size()){
									tempCounter = 0;
								}
								try{
									roundOffFigure = (int)Double.parseDouble(parts[1]);
								}catch(NumberFormatException nf){
									roundOffFigure= Integer.parseInt(parts[1]);
								}
								
								
								values.add(new Bar(roundOffFigure, colorArray.get(tempCounter), parts[0]));
								tempCounter++;						    
							    
							}else{
								break;
							}
							
						}
						System.out.println("after loop");
						int primaryIncrements = 100; 
					    int secondaryIncrements = 50; 
					    int tertiaryIncrements = 100;
					    Axis yAxis = new Axis(1000, 0, primaryIncrements, secondaryIncrements, 
					                         tertiaryIncrements, "Number of positive tweets");
					     
					    BarChart barChart = new BarChart(values, yAxis);
					    //barChart.yAxisStr = "Number of positive tweets";
					    barChart.xAxis = "Airline";
					    barChart.title = "Top 3 airlines with the most number of positive tweets";
					    barChart.setPreferredSize(new Dimension(952, 350));
					    panel.removeAll();
					    panel.updateUI();
					    panel.add(barChart);
					    panel.revalidate();
					    validate();
					}
					else if (selection == 0){
						int tempCounter = 0;
						ArrayList<Bar> values = new ArrayList<Bar>();
						while(true){
							line = br.readLine();
							if(line!=null){
								String parts[]=line.split("\t");
								String str = textArea.getText();
								textArea.setText(str + line + "\n");
								if(tempCounter>=colorArray.size()){
									tempCounter = 0;
								}
								String xAxisString = parts[0];
								if(xAxisString.length() > 8){
									xAxisString = parts[0].substring(0, 8);
								}	
								values.add(new Bar(Integer.parseInt(parts[3]), colorArray.get(tempCounter), xAxisString));
								tempCounter++;
							}else{
								break;
							}
							
						}
						int primaryIncrements = 500; 
					    int secondaryIncrements = 250; 
					    int tertiaryIncrements = 500;
					    Axis yAxis = new Axis(4000, 0, primaryIncrements, secondaryIncrements, 
					                         tertiaryIncrements, "How many negative reasons");
					     
					    BarChart barChart = new BarChart(values, yAxis);
					    barChart.yAxisStr = "Number of occurence";
					    barChart.xAxis = "Airline";
					    barChart.title = "How many negative reasons";
					    barChart.setPreferredSize(new Dimension(952, 350));
					    panel.removeAll();
					    panel.updateUI();
					    panel.add(barChart);
					    panel.revalidate();
					    validate();
					}
					else{
						while(true){
							line = br.readLine();
							if(line!=null){
								String str = textArea.getText();
								textArea.setText(str + line + "\n");
							}else{
								break;
							}
						}
					}
					br.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (URISyntaxException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		});
		Container cp = getContentPane();
	    
	    textArea.setSize(400,400);    
	    
	    	        textArea.setLineWrap(true);
	    	        textArea.setEditable(false);
	    	        textArea.setVisible(true);
	    	        	    contentPane.setLayout(null);
	    	        
	    	        	    JScrollPane scroll = new JScrollPane (textArea);
	    	        	    scroll.setBounds(0, 105, 1042, 353);
	    	        	    scroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
	    	        	    scroll.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
	    	        	    
	    	        	    	    contentPane.add(scroll);
	    comboBox.setBounds(0, 70, 121, 22);
	    cp.add(comboBox);
	    cp.add(runTaskButton);
	    JLabel lblNewLabel = new JLabel("Twitter Data Analysis");
	    lblNewLabel.setFont(new Font("Tahoma", Font.PLAIN, 32));
	    lblNewLabel.setBounds(0, 13, 388, 44);
	    contentPane.add(lblNewLabel);
	    contentPane.add(panel);
	   	contentPane.setVisible(true);
	}
	
	public class BarChart extends JPanel {
		  //offsets (padding of actual chart to its border)
	    int leftOffset = 140;
	    int topOffset = 120;
	    int bottomOffset = 100;
	    int rightOffset = 15;

	    //height of X labels (must be significantly smaller than bottomOffset)
	    int xLabelOffset = 40; 
	    //width of Y labels (must be significantly smaller than leftOffset)
	    int yLabelOffset = 40; 

	    //tick widths
	    int majorTickWidth = 10;
	    int secTickWidth = 5;
	    int minorTickWidth = 2;

	    String xAxis = "X Axis";
	    String yAxisStr = "Y Axis";
	    String title = "My Fruits";

	    int width = 952; //total width of the component
	    int height = 350; //total height of the component

	    Color textColor = Color.BLACK;
	    Color backgroundColor = Color.WHITE;

	    Font textFont = new Font("Arial", Font.BOLD, 20);
	    Font yFont = new Font("Arial", Font.PLAIN, 12);
	    Font xFont = new Font("Arial", Font.BOLD, 12);
	    Font titleFont = new Font("Arial", Font.BOLD, 18);

	    Font yCatFont = new Font("Arial", Font.BOLD, 12);
	    Font xCatFont = new Font("Arial", Font.BOLD, 12);

	    ArrayList<Bar> bars;
	    Axis yAxis;
	    int barWidth = 30;
	    
	    /**
	     * Construct BarChart
	     * 
	     * @param bars a number of bars to display
	     * @param yAxis Axis object describes how to display y Axis 
	     */
	    BarChart(ArrayList bars, Axis yAxis) {
	        this.bars = bars;
	        this.yAxis = yAxis;
	        this.yAxisStr = yAxis.yLabel;
	    }

	    @Override
	    protected void paintComponent(Graphics g) {

	        Graphics2D g2d = (Graphics2D) g;
	        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, // Anti-alias!
	                RenderingHints.VALUE_ANTIALIAS_ON);

	        g.drawRect(0, 0, width, height);
	        g2d.setColor(backgroundColor);
	        g.fillRect(0, 0, width, height);
	        g2d.setColor(Color.BLACK);

	        int heightChart = height - (topOffset + bottomOffset);
	        int widthChart = width - (leftOffset + rightOffset);

	        //left
	        g.drawLine(leftOffset, topOffset, leftOffset, heightChart + topOffset);

	        //bottom
	        g.drawLine(leftOffset, heightChart + topOffset, leftOffset + widthChart, heightChart + topOffset);

	        if (this.yAxis.primaryIncrements != 0)
	            drawTick(heightChart, this.yAxis.primaryIncrements, g, Color.BLACK, majorTickWidth);
	        if (this.yAxis.secondaryIncrements != 0)
	            drawTick(heightChart, this.yAxis.secondaryIncrements, g, Color.BLACK, secTickWidth);
	        if (this.yAxis.tertiaryIncrements != 0)
	            drawTick(heightChart, this.yAxis.tertiaryIncrements, g, Color.BLACK, minorTickWidth);

	        drawYLabels(heightChart, this.yAxis.primaryIncrements, g, Color.BLACK);

	        drawBars(heightChart, widthChart, g);

	        drawLabels(heightChart, widthChart, g);
	    }

	    private void drawTick(int heightChart, int increment, Graphics g, Color c, int tickWidth) {

	        int incrementNo = yAxis.maxValue / increment;

	        double factor = ((double) heightChart / (double) yAxis.maxValue);

	        double incrementInPixel = (double) (increment * factor);

	        g.setColor(c);

	        for (int i = 0; i < incrementNo; i++) {
	            int fromTop = heightChart + topOffset - (int) (i * incrementInPixel);
	            g.drawLine(leftOffset, fromTop, leftOffset + tickWidth, fromTop);
	        }
	    }
	    
	    private void drawYLabels(int heightChart, int increment, Graphics g, Color c) {

	        int incrementNo = yAxis.maxValue / increment;

	        double factor = ((double) heightChart / (double) yAxis.maxValue);

	        int incrementInPixel = (int) (increment * factor);

	        g.setColor(c);
	        FontMetrics fm = getFontMetrics(yCatFont);

	        for (int i = 0; i < incrementNo; i++) {
	            int fromTop = heightChart + topOffset - (i * incrementInPixel);

	            String yLabel = "" + (i * increment);

	            int widthStr = fm.stringWidth(yLabel);
	            int heightStr = fm.getHeight();

	            g.setFont(yCatFont);
	            g.drawString(yLabel, (leftOffset - yLabelOffset) + (yLabelOffset/2 - widthStr/2), fromTop + (heightStr / 2));
	        }
	    }

	    private void drawBars(int heightChart, int widthChart, Graphics g) {

	        int i = 0;
	        int barNumber = bars.size();

	        int pointDistance = (int) (widthChart / (barNumber + 1));

	        for (Bar bar : bars) {

	            i++;

	            double factor = ((double) heightChart / (double) yAxis.maxValue);

	            int scaledBarHeight = (int) (bar.value * factor);

	            int j = topOffset + heightChart - scaledBarHeight;

	            g.setColor(bar.color);
	            g.fillRect(leftOffset + (i * pointDistance) - (barWidth / 2), j, barWidth, scaledBarHeight);

	            //draw tick
	            g.drawLine(leftOffset + (i * pointDistance),
	                    topOffset + heightChart,
	                    leftOffset + (i * pointDistance),
	                    topOffset + heightChart + 2);

	            FontMetrics fm = getFontMetrics(xCatFont);
	            int widthStr = fm.stringWidth(bar.name);
	            int heightStr = fm.getHeight();

	            g.setFont(xCatFont);
	            g.setColor(Color.BLACK);

	            int xPosition = leftOffset + (i * pointDistance) - (widthStr / 2);
	            int yPosition = topOffset + heightChart + xLabelOffset - heightStr/2;

	            //draw tick
	            g.drawString(bar.name, xPosition, yPosition);
	        }
	    }

	    private void drawLabels(int heightChart, int widthChart, Graphics g) {

	        Graphics2D g2d = (Graphics2D)g;

	        AffineTransform oldTransform = g2d.getTransform();

	        FontMetrics fmY = getFontMetrics(yFont);
	        int yAxisStringWidth = fmY.stringWidth(yAxisStr);
	        int yAxisStringHeight = fmY.getHeight();

	        FontMetrics fmX = getFontMetrics(xFont);
	        int xAxisStringWidth = fmX.stringWidth(yAxisStr);
	        int xAxisStringHeight = fmX.getHeight();

	        FontMetrics fmT = getFontMetrics(titleFont);
	        int titleStringWidth = fmT.stringWidth(title);
	        int titleStringHeight = fmT.getHeight();

	        g2d.setColor(Color.BLACK);
	        //draw tick
	        g2d.rotate(Math.toRadians(270)); //rotates to above out of screen.

	        int translateDown = -leftOffset -(topOffset + heightChart/2 + yAxisStringWidth/2);

	        //starts off being "topOffset" off, so subtract that first
	        int translateLeft = -topOffset + (leftOffset-yLabelOffset)/2 + yAxisStringHeight/2;

	        //pull down, which is basically the left offset, topOffset, then middle it by 
	        //usin chart height and using text height.
	        g2d.translate(translateDown, translateLeft);

	        g2d.setFont(yFont);
	        g2d.drawString(yAxisStr, leftOffset, topOffset);

	        //reset
	        g2d.setTransform(oldTransform);

	        int xAxesLabelHeight = bottomOffset - xLabelOffset;

	        //x label        
	        g2d.setFont(xFont);
	        g2d.drawString(xAxis, widthChart/2 + leftOffset - xAxisStringWidth/2, topOffset + heightChart + xLabelOffset + xAxesLabelHeight/2);

	                g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, // Anti-alias!
	                RenderingHints.VALUE_ANTIALIAS_ON);
	        //title
	        g2d.setFont(titleFont);
	        int titleX = (leftOffset + rightOffset + widthChart)/2 - titleStringWidth/2;
	        int titleY = topOffset/2 + titleStringHeight/2;
	        System.out.println("titleStringHeight " + titleStringHeight);
	        System.out.println("titleX " + titleX);
	        System.out.println("titleY " + titleY);
	        System.out.println("topOffset " + topOffset);

	        g2d.drawString(title, titleX, titleY);
	    }
	}
	    
	    /**
	     * @copyright 2014 
	     * @author Oliver Watkins (www.blue-walrus.com) 
	     * 
	     * All Rights Reserved
	     */
	    public class Axis {

	        int primaryIncrements = 0; 
	        int secondaryIncrements = 0;
	        int tertiaryIncrements = 0;

	        int maxValue = 100;
	        int minValue = 0;

	        String yLabel;

	        Axis(String name) {
	            this(100, 0, 50, 10, 5, name);
	        }

	        Axis(int primaryIncrements, int secondaryIncrements, int tertiaryIncrements, String name) {
	            this(100, 0, primaryIncrements, secondaryIncrements, tertiaryIncrements, name);
	        }

	        Axis(Integer maxValue, Integer minValue, int primaryIncrements, int secondaryIncrements, int tertiaryIncrements, String name) {

	            this.maxValue = maxValue; 
	            this.minValue = minValue;
	            this.yLabel = name;

	            if (primaryIncrements != 0)
	                this.primaryIncrements = primaryIncrements; 
	            if (secondaryIncrements != 0)
	                this.secondaryIncrements = secondaryIncrements;
	            if (tertiaryIncrements != 0)
	                this.tertiaryIncrements = tertiaryIncrements;
	        }
	    }
	    
	    public class Bar {

	        double value; 
	        Color color;
	        String name;

	        Bar(int value, Color color, String name) {
	            this.value = value;
	            this.color = color;
	            this.name = name;
	        }
	    }
	
}
