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


public class TransMaxJuly {

	public static class TransMaxJulyMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
		{
			String trans[] = value.toString().split(",");
			
			String month = trans[1];
			
			String cust_id = trans[0];
			double amt = Double.parseDouble(trans[3]);
			
			
			String month_cust = month + "," + cust_id;
	
			context.write(new Text(month_cust), new DoubleWritable(amt));
		}
	}
	
	public static class TransMaxJulyPartitioner extends Partitioner<Text,Text>
	{
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks)
		{
			
			String custmonthSplit[] = key.toString().split(",");
	
			String monthSplit[] = custmonthSplit[0].split("-");
			String monthsplitVal = monthSplit[0];
			
			
			
			
			if(monthsplitVal.equals("07"))
				
				return 0%numReduceTasks;
			
			else return numReduceTasks;	
			
		}
	}
	
	public static class TransMaxJulyReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,InterruptedException
		{
			double maxAmt = 0.00;
			double tempAmt = 0.00;
			
			for(DoubleWritable val : values)
			{
				tempAmt = val.get();
				
				if(tempAmt > maxAmt)
					
					maxAmt = tempAmt;
			
			context.write(new Text(key), new DoubleWritable(maxAmt));
			}
		}
	}
	
	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TransMaxJuly.class);
		job.setMapperClass(TransMaxJulyMapper.class);
		job.setPartitionerClass(TransMaxJulyPartitioner.class);
		job.setReducerClass(TransMaxJulyReducer.class);
		job.setNumReduceTasks(2);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}


