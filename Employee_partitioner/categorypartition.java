


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


	public class txnpartitition extends Configured implements Tool
	{
			
			public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
			   {
			      public void map(LongWritable key, Text value, Context context)
			      {	    	  
			         try{
			            String[] str = value.toString().split(",");	 
			            String field= str[4];
			          //  String val=str[0]+','+str[1]+','+str[2]+','+str[3]+','+str[5]+','+str[6]+','+str[7]+','+str[8];
			            context.write(new Text(field),new Text(value));
			           
			         }
			         catch(Exception e)
			         {
			            System.out.println(e.getMessage());
			         }
			      }
			   }
			
			  public static class ReduceClass extends Reducer<Text,Text,Text,Text>
			   {
				    public void reduce(Text key, Text value,Context context) throws IOException, InterruptedException {
			       
				    	context.write(new Text(key),new Text(value));
				      
				 }
			   }
			   
			 
			  public static class Coderpartitioner extends Partitioner <Text, Text>
			  {
				  public int getPartition(Text key, Text value, int numReducetasks)
				  {
					  String[] str= value.toString().split(",");
					 
					if(str[4].equalsIgnoreCase("Exercise & Fitness")) {
						return 0;
					}
					else if(str[4].equalsIgnoreCase("Gymnastics")) {
						return 1;
					}
					else if(str[4].equalsIgnoreCase("Team sports")) {
						return 2;
					}
					else if(str[4].equalsIgnoreCase("outdoor Recreation")) {
						return 3;
					}
					else if(str[4].equalsIgnoreCase("Puzzles")) {
						return 4;
					}
					else if(str[4].equalsIgnoreCase("Outdoor Play Equipment")) {
						return 5;
					}
					else if(str[4].equalsIgnoreCase("Winter Sports")) {
				    return 6;
					}
					else if(str[4].equalsIgnoreCase("Jumping")) {
						return 7;
					}
					else if(str[4].equalsIgnoreCase("Indoor Games")) {
						return 8;
					}
					else if(str[4].equalsIgnoreCase("Combat sports")) {
						return 9;
					}
					else if(str[4].equalsIgnoreCase("water Sports")) {
						return 10;
					}
					else if(str[4].equalsIgnoreCase("Air Sports")) {
						return 11;
					}
					else if(str[4].equalsIgnoreCase("Racquet Sports")) {
						return 12;
					}
					else if(str[4].equalsIgnoreCase("Dancing")) {
						return 13;
					}
					else {
						return 14;
					}
				  }
			  }

			public int run(String[] args) throws Exception {
				    Configuration conf = new Configuration();
				    //conf.set(name", "value")
				    Job job = Job.getInstance(conf);
				    job.setJarByClass(txnpartitition.class);
				    job.setJobName("");
				    FileInputFormat.setInputPaths(job, new Path(args[0]));
				    FileOutputFormat.setOutputPath(job, new Path(args[1]));
				    
				    job.setMapperClass(MapClass.class);
				    job.setMapOutputKeyClass(Text.class);
				    job.setMapOutputValueClass(Text.class);
				    
				    job.setPartitionerClass(Coderpartitioner.class);
				    job.setReducerClass(ReduceClass.class);
				    job.setNumReduceTasks(15);
				    job.setInputFormatClass(TextInputFormat.class);
				    job.setOutputFormatClass(TextOutputFormat.class);
				    
				   
				    job.setOutputKeyClass(Text.class);
				    job.setOutputValueClass(Text.class);
				    
				    System.exit(job.waitForCompletion(true)? 0 : 1);
				    return 0;
				  }
			public static void main(String ar[]) throws Exception
			   {
			      int res = ToolRunner.run(new Configuration(), new txnpartitition(),ar);
			      System.exit(0);
			   }
			}
	
	
