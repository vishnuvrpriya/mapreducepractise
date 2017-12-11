import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;



public class MapsideJoin 
{	
	public static class MPJoinMapper extends Mapper<LongWritable, Text, Text, IntWritable> 

	{

		
		private Map<String,String> storeTable = new HashMap<String,String>();
		
		private Text outputKey=new Text();
		
		
		protected void setup(Context context) throws IOException,InterruptedException
		
		{
			super.setup(context);
			
			URI[] files = context.getCacheFiles();
		
			Path p1= new Path(files[0]);
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			
			
					
			if(p1.getName().equals("store_master"))
			{
				
				BufferedReader readers = new BufferedReader(new InputStreamReader(fs.open(p1)));
				
				String lines = readers.readLine();
				
				while(lines!= null)
				{
					String[] tokens = lines.split(",");
					
					String store_id = tokens[0];
					String state = tokens[2];
					
					
					storeTable.put(store_id,state);
					lines = readers.readLine();
						
				}
				
					readers.close();
			}
					
			
			
			if(storeTable.isEmpty())
			{
				throw new IOException("Unable to load store master data");
			
			}
				}
			
			protected void map(LongWritable key,Text value,Context context)
					throws java.io.IOException, InterruptedException
			{
				
				String row = value.toString();
				String[] tokenes = row.split(",");
				
				String store_id = tokenes[0];
				String pdt_id= tokenes[1];
				
				String state = storeTable.get(store_id);
			
				String myKey = store_id+","+pdt_id+","+state;
				
				int pdt_quantity = Integer.parseInt(tokenes[2]);
				
				outputKey.set(myKey);
				
				context.write(outputKey,new IntWritable(pdt_quantity));
				
			}
	}

			
			public static class CaderPartitioner extends
			   Partitioner < Text, IntWritable >
			   {
			      @Override
			      public int getPartition(Text key, IntWritable value, int numReduceTasks)
			      {
			         String[] partip= key.toString().split(",");
			         String state = partip[0];


			         if(state.contains("MAH"))
			         {
			            return 0 % numReduceTasks;
			         }
			         else if (state.contains("KAR"))
			         {
			            return 1 % numReduceTasks ;
			         }
					
			         else return 0;
			      }
			   }
			      
			 public static class MPJoinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
				 
				 		IntWritable totalsales = new IntWritable();
			    		

						public void reduce(Text key, Iterable<IntWritable> values, Context context)
			    				throws IOException, InterruptedException {
			    			
			    			
			    			
			    			int sum=0;
			    			
			    			for(IntWritable val : values)
			    			{
			    				/*String[] str = val.toString().split(",");
			    				int qty = Integer.parseInt(str[2]);
			    				if (qty > sum)
			    				{
			    					sum += qty;
			    				}*/
			    				sum += val.get();
			    			}
			    			
			    			totalsales.set(sum);
			    			context.write(key, totalsales);
			    		}	
			 }
			      
			   
			
		
	
		public static void main(String [] args)
			throws IOException,	ClassNotFoundException,InterruptedException
			{
			
			Configuration conf = new Configuration();
			conf.set("mapreduce.textOutputformat.seperator","/t");
			Job job = Job.getInstance(conf);
			job.setJarByClass(MapsideJoin.class);
			job.setMapperClass(MPJoinMapper.class);
			job.setPartitionerClass(CaderPartitioner.class);
			job.setReducerClass(MPJoinReducer.class);
			job.addCacheFile(new Path(args[1]).toUri());
			job.setNumReduceTasks(2);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job,new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[2]));
			
			job.waitForCompletion(true);
			
			}
	
}
