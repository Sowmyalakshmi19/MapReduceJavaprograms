

import java.io.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class filtersalesdata {
			
			public static class MapClass extends Mapper<LongWritable,Text,Text,NullWritable>
			   {
				
				private Text sentence = new Text();
			      public void map(LongWritable key, Text value, Context context) 
			    		  throws IOException,InterruptedException {
			          String mysearchtext= context.getConfiguration().get("mytext");
			          String line= value.toString();
			          if(mysearchtext !=null)
			          {
			        	  if(line.contains(mysearchtext))
			        	  {
			        		  sentence.set(line);
			        	  
			        	 context.write(sentence, NullWritable.get());
			         }
			      }
			   }
			   }
			
			 
			  public static void main(String[] args) throws Exception {
				    Configuration conf = new Configuration();
				    if(args.length > 2)
				    {
				    	conf.set("mytext", args[2]);
				    }
				    Job job = Job.getInstance(conf,"filtersalesdata");
				    job.setJarByClass(filtersalesdata.class);
				    job.setMapperClass(MapClass.class);
				    //job.setCombinerClass(ReduceClass.class);
				    //job.setReducerClass(ReduceClass.class);
				    job.setNumReduceTasks(0);
				    job.setMapOutputKeyClass(Text.class);
				    job.setMapOutputValueClass(NullWritable.class);
				    FileInputFormat.addInputPath(job, new Path(args[0]));
				    FileOutputFormat.setOutputPath(job, new Path(args[1]));
				    System.exit(job.waitForCompletion(true) ? 0 : 1);
				  }
			   }

	
