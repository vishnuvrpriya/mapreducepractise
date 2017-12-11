import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;



import org.apache.hadoop.filecache.DistributedCache;
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



@SuppressWarnings("deprecation")
public class TransProff {
	
	public static class TransProffMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		HashMap<String,String> custProff = new HashMap<String,String>();
		
		public void setup(Context context) throws IOException,InterruptedException
		{
			Path[] allFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());	
			
				for(Path eachFile : allFiles)
				{
					if(eachFile.getName().equals("Customer.dat"))
					{
						FileReader fr =  new FileReader(eachFile.toString());
						BufferedReader br = new BufferedReader(fr);
						String reader = br.readLine();
						
						while(reader!= "null")
						{
							String custVals[] = eachFile.toString().split(",");
							
							String cust_id = custVals[0];
							String cust_proff = custVals[1];
							
							custProff.put(cust_id, cust_proff);
							reader = br.readLine();
						}
						
						br.close();	
						}
						
					}
				}
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
		{
			String transVals[] = value.toString().split(",");
			
			String cust_id = transVals[2];
			double amt = Double.parseDouble(transVals[3]);
			
			String proff = custProff.get(cust_id);
			
			String mKey = cust_id + "," + proff;
			
			context.write(new Text(mKey), new DoubleWritable(amt));
			
		}
	}
	
	public static class TransProffReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,InterruptedException
		{
			double maxAmt = 0.00;
			double amtVal = 0.00;
			
			
			for(DoubleWritable tempVal : values)
			{
				
				amtVal = tempVal.get();
				
				if(amtVal > maxAmt)
				{
					maxAmt = amtVal;
				}
			}
		}
	}
	
	public static void main(String[] args) throws IOException,InterruptedException, URISyntaxException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TransProff.class);
		job.setMapperClass(TransProffMapper.class);
		job.setReducerClass(TransProffReducer.class);
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
