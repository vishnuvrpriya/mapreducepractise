import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TxnsSalesMonth {

	public static class TxnsSalesMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
		{
			String trans[] = value.toString().split(",");
			
			String month = trans[1];
			double amt = Double.parseDouble(trans[3]);
			
			context.write(new Text(month), new DoubleWritable(amt));
		}
	}
	
	public static class TxnsSalesPartitioner extends Partitioner<Text,DoubleWritable>
	{
		@Override
		public int getPartition(Text key, DoubleWritable value, int numReduceTasks)
		{
			
			String monthSplit[] = key.toString().split("-");
			String monthVal = monthSplit[0];
			
			
			if(monthVal.equals("01"))
				
				return 0%numReduceTasks;
			
			if(monthVal.equals("02"))
				
				return 0%numReduceTasks;
			
			if(monthVal.equals("03"))
				
				return 0%numReduceTasks;
			
			if(monthVal.equals("04"))
				
				return 0%numReduceTasks;
			
			if(monthVal.equals("05"))
				
				return 0%numReduceTasks;
			
			if(monthVal.equals("06"))
				
				return 0%numReduceTasks;
			
			if(monthVal.equals("07"))
				
				return 0%numReduceTasks;
			
			if(monthVal.equals("08"))
				
				return 0%numReduceTasks;
			
			if(monthVal.equals("09"))
				
				return 0%numReduceTasks;
			
			if(monthVal.equals("10"))
				
				return 0%numReduceTasks;
			
			if(monthVal.equals("11"))
				
				return 0%numReduceTasks;
			
			if(monthVal.equals("12"))
				
				return 0%numReduceTasks;
			
			else return numReduceTasks;	
			
		}
	}
	
	public static class TxnsSalesReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,InterruptedException
		{
			double sumAmt = 0.0;
			
			for(DoubleWritable val : values)
			{
				sumAmt += val.get();
			}
			
			context.write(new Text(key), new DoubleWritable(sumAmt));
		}
	}
	
	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TxnsSalesMonth.class);
		job.setMapperClass(TxnsSalesMapper.class);
		job.setPartitionerClass(TxnsSalesPartitioner.class);
		job.setReducerClass(TxnsSalesReducer.class);
		job.setNumReduceTasks(12);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
