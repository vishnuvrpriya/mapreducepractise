import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TxnsMonthWise {

	public static class TxnsMonthMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
		{
			String trans[] = value.toString().split(",");
			
			String month = trans[1];
			
			String cust_id = trans[0];
			String trans_id = trans[2];
			String amt = trans[3];
			String cat = trans[4];
			String eqip = trans[5];
			String state = trans[6];
			String country = trans[7];
			String pymtmode = trans[8];
			
		
	String txns = cust_id + "," + trans_id + "," + amt + "," + cat + "," + eqip + "," + state + "," + country + "," + pymtmode; 
			
			context.write(new Text(month), new Text(txns));
		}
	}
	
	public static class TxnsMonthPartitioner extends Partitioner<Text,Text>
	{
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks)
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
	
	public static class TxnsMonthReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException
		{
			
			
			for(Text val : values)
			{
			
			context.write(new Text(key), new Text(val));
			}
		}
	}
	
	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TxnsMonthWise.class);
		job.setMapperClass(TxnsMonthMapper.class);
		job.setPartitionerClass(TxnsMonthPartitioner.class);
		job.setReducerClass(TxnsMonthReducer.class);
		job.setNumReduceTasks(12);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}

