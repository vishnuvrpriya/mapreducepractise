import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;



public class TxnsAmtBtw {
	
	public static class TxnsBtwMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
		{
			String trans[] = value.toString().split(",");
			
			String trans_id  = trans[2];
			double amt = Double.parseDouble(trans[3]);
			
			
			context.write(new Text(trans_id), new DoubleWritable(amt));
		}
	}
	
	
	public static class TxnsBtwReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,InterruptedException
		{
			
			double records = 0.00;
			
			for(DoubleWritable val : values)
			{
				records = val.get();
				
				if((records >= 175.00) && (records<=200.00))
				{
					context.write(key, new DoubleWritable(records));
				}
			}
		}
	}
	
	public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TxnsAmtBtw.class);
		job.setMapperClass(TxnsBtwMapper.class);
		job.setReducerClass(TxnsBtwReducer.class);
		//job.setNumReduceTasks(2);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
				
	}

}
