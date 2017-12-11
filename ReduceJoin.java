import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class ReduceJoin {

	public static class PurchaseMapper extends Mapper<LongWritable,Text,Text,Text>
	
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[0]), new Text("purchase\t"+parts[1]));
			
		}
		
	}
	
	public static class SalesMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[0]), new Text("sales\t"+parts[1]));
		}
	}
	
	public static class CommonReducer extends Reducer<Text,Text,Text,Text>
	{
		
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
		{
			double totalpurchase=0.0;
			double totalsales=0.0;
			
			for(Text t :values)
			{
				String parts[] = t.toString().split("\t");
				
				
				if(parts[0].equals("purchase"))
				{
					totalpurchase=totalpurchase+Float.parseFloat(parts[1]);
					
				}else if(parts[0].equals("sales"))
				{
					totalsales= totalsales+Float.parseFloat(parts[1]);	
					
				}
				
			}
				
			String str = String.format("%f\t%f", totalpurchase, totalsales);
			
				context.write(key, new Text(str));
				
			}
		}
		
	public static void main(String [] args)
			throws IOException,	ClassNotFoundException,InterruptedException
			{
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setJarByClass(ReduceJoin.class);
			job.setReducerClass(CommonReducer.class);
			job.setJobName("Reducer side Join");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			//job.setNumReduceTasks(0);
			MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,PurchaseMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,SalesMapper.class);
			FileOutputFormat.setOutputPath(job,new Path(args[2]));
			job.waitForCompletion(true);
			
			}
	
	}

