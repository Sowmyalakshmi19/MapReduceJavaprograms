


import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


	public class totalamount {
			
			public static class MapClass extends Mapper<LongWritable,Text,Text,FloatWritable>
			   {
			      public void map(LongWritable key, Text value, Context context)
			      {	    	  
			         try{
			            String[] str = value.toString().split(",");	 
			            //long val1 = Long.parseLong(str[2]);
			           // Text id = new Text(str[2]);
			            float val2 = Float.parseFloat(str[3]);
			           // long val1=long.valueOf(record[2]);
						//long val2= long.valueOf(record[3]);
			            context.write(new Text(str[2]),new FloatWritable(val2));
			            
			         }
			         catch(Exception e)
			         {
			            System.out.println(e.getMessage());
			         }
			      }
			   }
			
			  public static class ReduceClass extends Reducer<Text,FloatWritable,Text,FloatWritable>
			   {
				    private FloatWritable result = new FloatWritable();
				    
				    public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
				      long sum = 0;
						
				         for (FloatWritable val : values)
				         {       	
				        	sum += val.get();  
				        	
				         }
				     
				      result.set(sum);
				      
				    }
			   }
			  public static void main(String[] args) throws Exception {
				    Configuration conf = new Configuration();
				    conf.set("name", "value");
				    Job job = Job.getInstance(conf, "totalamount");
				    job.setJarByClass(totalamount.class);
				    job.setMapperClass(MapClass.class);
				    //job.setCombinerClass(ReduceClass.class);
				    job.setReducerClass(ReduceClass.class);
				    job.setNumReduceTasks(0);
				    job.setMapOutputKeyClass(Text.class);
				    job.setMapOutputValueClass(FloatWritable.class);
				    job.setOutputKeyClass(Text.class);
				    job.setOutputValueClass(FloatWritable.class);
				    FileInputFormat.addInputPath(job, new Path(args[0]));
				    FileOutputFormat.setOutputPath(job, new Path(args[1]));
				    System.exit(job.waitForCompletion(true) ? 0 : 1);
				  }
		}
