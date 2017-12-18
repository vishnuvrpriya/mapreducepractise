import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TxnsAvgAmt {
	
	public static class TxnsAvgMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
		{
			String trans[] = value.toString().split(",");
			
			String cust_id  = trans[0];
			double amt = Double.parseDouble(trans[3]);
			
			
			context.write(new Text(cust_id), new DoubleWritable(amt));
		}
	}
	
	
	public static class TxnsAvgReducer extends Reducer<Text,DoubleWritable,Text,Text>
	{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,InterruptedException
		{
			
			double sumAmt = 0.00;
			int countAmt=0;
			double avgAmt=0.00;
			
			for(DoubleWritable val : values)
			{
				sumAmt+= val.get();
				
				
			}
				countAmt++;
				avgAmt = sumAmt/countAmt;
			
			
				
	String avg = "The sum and average amount of transactions for each cust ID is"+ "," +String.format("%f\t%f", sumAmt,avgAmt);
				
				context.write(key, new Text(avg));
				
		}
	}
	
	public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TxnsAvgAmt.class);
		job.setMapperClass(TxnsAvgMapper.class);
		job.setReducerClass(TxnsAvgReducer.class);
		//job.setNumReduceTasks(2);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
				
	}


}


