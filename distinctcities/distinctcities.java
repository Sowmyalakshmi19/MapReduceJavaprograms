

import java.io.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


	public class distinctcities {
		
		public static class MapClass extends Mapper<LongWritable,Text,Text,NullWritable>
		   {
		      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		      {	    	  
		         
		            String record = value.toString(); 
		            String[] parts= record.split(",");
		            String city= parts[3];
		            context.write(new Text(city), NullWritable.get());
		            
		         }
		         
		   }
		
		  public static class ReduceClass extends Reducer<Text,NullWritable,Text,NullWritable>
		   {
			  
			    public void reduce(Text key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
			      	      
			      context.write(key, NullWritable.get());
			      
			    }
		   }
		  public static void main(String[] args) throws Exception {
			    Configuration conf = new Configuration();
			    //conf.set("name", "value")
			    Job job = Job.getInstance(conf, "Distinct Cities");
			    job.setJarByClass(distinctcities.class);
			    job.setMapperClass(MapClass.class);
			    //job.setCombinerClass(ReduceClass.class);
			    job.setReducerClass(ReduceClass.class);
			    //job.setNumReduceTasks(2);
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(NullWritable.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }
	}
