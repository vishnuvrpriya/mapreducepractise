import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

	public class RetailCalc
	{
		public static class RetailMapper extends Mapper<LongWritable,Text,Text,IntWritable> 
	{
	
			public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException 
			{
				final IntWritable sales = new IntWritable();
			try 
				{
			
			String[] retail = value.toString().split(";");
			
			String tns_date = retail[0];
			String cust_id = retail[1];
			
			String myKey = tns_date + "," + cust_id;
			
			sales.set(Integer.parseInt(retail[8]));
			
			context.write(new Text(myKey),sales);
			
				}catch (IndexOutOfBoundsException e) {
				}catch (ArithmeticException e1) {
				}	
			}
	
	}

		public static class RetailReducer extends Reducer<Text,IntWritable,Text,IntWritable>
		{
			int amt_val=0;
			int maxAmt=0;
			IntWritable maxSales = new IntWritable(); 
			
			public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException
			{
				try
				{
					for(IntWritable val : values)
					{
						amt_val= val.get();
						
						if(amt_val > maxAmt) {
							maxAmt = amt_val;
						}
					}
						
					maxSales.set(maxAmt);
					
					context.write(key,maxSales);
				
				}catch(IndexOutOfBoundsException e) {
				}catch (ArithmeticException e1) {
				}	
			
			}
		}
		
		public static void main(String[] args)
		throws IOException,InterruptedException,ClassNotFoundException
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setJarByClass(RetailCalc.class);
			job.setMapperClass(RetailMapper.class);
			job.setReducerClass(RetailReducer.class);
			//job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.setInputPaths(job,new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[1]));
			
			job.waitForCompletion(true);
			
			
			
		}
}