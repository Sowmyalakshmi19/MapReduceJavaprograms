

import java.io.*;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
//import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


  public class maxmodule {
				public static class MapClass extends Mapper<LongWritable,Text,Text, IntWritable>
				   {
					private final static IntWritable one= new IntWritable(1);
				   
					public void map(LongWritable key, Text value, Context context)
				    		  throws IOException, InterruptedException {
						try {
				    	    String[] str = value.toString().split(" ");	 
				    	    if (str[3].equals("[ERROR]")) {
				    	    	String mod1= str[2] + ',' + str[3];  
				    	     Text mod= new Text(mod1);
				    	    context.write(mod, one);
				            
				         }
						}
						 catch(Exception e)
				         {
				            System.out.println(e.getMessage());
				         }
				      }
				   }


			 //Reducer class
	public static class maxReduce extends Reducer<Text,IntWritable,NullWritable,Text>
		{
	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	public void reduce(Text key, Iterable <IntWritable> values, Context context) throws IOException, InterruptedException
							      {
							         int sum = 0;
							         String myValue = "";
							         String mySum = "";
							         String module="";
							         for (IntWritable val : values)
							         {
							        	 String[] token= key.toString().split(","); 
							        	 module=token[0];
							             sum += val.get();
							         	 
							         }
							       // myValue = key.toString();
							    
							        mySum = String.format("%d", sum);
							        myValue = module + ',' + mySum;
									
							        repToRecordMap.put(sum, new Text(myValue));
									
									if (repToRecordMap.size() > 1) 
										{
												repToRecordMap.remove(repToRecordMap.firstKey());
										}

							      }
						      
									protected void cleanup(Context context) throws IOException,
									InterruptedException 
									{
									
										for (Text t : repToRecordMap.values()) 
										{
												context.write(NullWritable.get(), t);
										}
									}
							      
							   }
							   
						//Main class
							   
							   public static void main(String[] args) throws Exception {
									
									Configuration conf = new Configuration();
									Job job = Job.getInstance(conf, "Top Buyer");
								    job.setJarByClass(maxmodule.class);
								    job.setMapperClass(MapClass.class);
								    job.setReducerClass(maxReduce.class);
								    job.setMapOutputKeyClass(Text.class);
								    job.setMapOutputValueClass(IntWritable.class);
								    job.setOutputKeyClass(NullWritable.class);
								    job.setOutputValueClass(Text.class);
								    FileInputFormat.addInputPath(job, new Path(args[0]));
								    FileOutputFormat.setOutputPath(job, new Path(args[1]));
								    System.exit(job.waitForCompletion(true) ? 0 : 1);
								  }
						}
