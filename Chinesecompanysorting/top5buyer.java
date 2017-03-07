

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


  public class top5buyer {
		public static class Top5Mapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
			public void map(LongWritable key, Text value, Context context
	        ) throws IOException, InterruptedException {
				try {
				String[] str = value.toString().split(";");
				String custid= str[1];
				long amount= Long.parseLong(str[8]);
				context.write(new Text(custid),new LongWritable(amount));
		           
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	

		public static class Top5Reducer extends
		Reducer<Text, LongWritable, NullWritable, Text> {
			private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

			public void reduce(Text key, Iterable<LongWritable> values,
					Context context) throws IOException, InterruptedException {
				long sum=0;
				String myvalue= "";
				String mysum= "";
					for (LongWritable val : values) {
						sum+= val.get();
					}
					myvalue= key.toString();
					mysum= String.format("%d", sum);
					myvalue= myvalue + ',' + mysum;
					repToRecordMap.put(new Long(sum), new Text(myvalue));
					if (repToRecordMap.size() > 1) {
					repToRecordMap.remove(repToRecordMap.firstKey());
							}
			}
			
					protected void cleanup(Context context) throws IOException,
					InterruptedException {
					for (Text t : repToRecordMap.values()) {
						// Output our five records to the file system with a null key
						context.write(NullWritable.get(), t);
						}
					}
			
		}
			
		public static void main(String[] args) throws Exception {
			
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Top record for largest amount spent");
		    job.setJarByClass(top5buyer.class);
		    job.setMapperClass(Top5Mapper.class);
		    job.setReducerClass(Top5Reducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(LongWritable.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	}
