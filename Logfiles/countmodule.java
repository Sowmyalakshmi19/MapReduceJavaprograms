


import java.io.*;
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



	public class countmodule {
			public static class MapClass extends Mapper<LongWritable,Text,Text, IntWritable>
			   {
				private final static IntWritable one= new IntWritable(1);
			   
				public void map(LongWritable key, Text value, Context context)
			    		  throws IOException, InterruptedException {
					try {
			    	    String[] str = value.toString().split(" ");	 
			    	    if ((str[3].equals("[TRACE]"))|| (str[3].equals("[ERROR]"))) {
			          context.write(new Text(str[3]),one);
			            
			         }
					}
					
			           
			         catch(Exception e)
			         {
			            System.out.println(e.getMessage());
			         }
			            
			      }
			   }
			public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
			  {
						   

							private IntWritable result = new IntWritable();
				
						    
						    public void reduce(Text key, Iterable<IntWritable> values,Context context) 
						    		throws IOException, InterruptedException {
				                         int sum =0;
				                   for(IntWritable val:values)
				                        {
					                 sum += val.get();
				                         }
				                     result.set(sum);
				                     context.write(key,result);
						    		}
						    	    }		   
					  public static void main(String[] args) throws Exception {
						    Configuration conf = new Configuration();
						    //conf.set("name", "value")
						    Job job = Job.getInstance(conf, "Vehicle");
						    job.setJarByClass(countmodule.class);
						    job.setMapperClass(MapClass.class);
						    //job.setCombinerClass(ReduceClass.class);
						    job.setReducerClass(ReduceClass.class);
						    //job.setNumReduceTasks(0);
						    job.setMapOutputKeyClass(Text.class);
						    job.setMapOutputValueClass(IntWritable.class);
						    job.setOutputKeyClass(Text.class);
						    job.setOutputValueClass(IntWritable.class);
						    FileInputFormat.addInputPath(job, new Path(args[0]));
						    FileOutputFormat.setOutputPath(job, new Path(args[1]));
						    System.exit(job.waitForCompletion(true) ? 0 : 1);
					  }
					   }



				
