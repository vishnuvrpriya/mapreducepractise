import java.io.IOException;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;




public class TransTop3 {
	
	public static class TransProffMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
		{
			String transVals[] = value.toString().split(",");
			
			String cust_id = transVals[2];
			double amt = Double.parseDouble(transVals[3]);
			
			context.write(new Text(cust_id), new DoubleWritable(amt));
			
		}
	}
	
	public static class TransProffReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,InterruptedException
		{
			double maxAmt = 0.00;
			double amtVal = 0.00;
			int count = 0;
			
			
			for(DoubleWritable tempVal : values)
			{
				
				if(count<=3)
				{
					amtVal = tempVal.get();
				
				if(amtVal > maxAmt)
				{
					maxAmt = amtVal;
				}
				
				count++;
				}
			}
		}
	}
	
	public static void main(String[] args) throws IOException,InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TransTop3.class);
		job.setMapperClass(TransTop3Mapper.class);
		job.setReducerClass(TransTop3Reducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		DistributedCache.addCacheFile(new URI("/Customer.dat"),job.getConfiguration());
		
		job.waitForCompletion(true);
	}
}

public class TransTop3 {

}
