import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.net.URI;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class SentimentAnalysis {

	public static class SentimentTokenizerMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		private Map<String,String> wordMap = new HashMap<String,String>();
		private final static IntWritable sum_value = new IntWritable();
		private Text word= new Text();
		String myWord = "";
		int myValue=0;
		
		public void setup(Context context) throws IOException,InterruptedException
		{
			super.setup(context);
			
			URI[] files = context.getCacheFiles();
			
			Path p = new Path(files[0]);
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			
			if(p.getName().equals("AFINN.txt"))
				{
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));
				
				String line = reader.readLine();
				while(line!=null)
				{
					String tokens[] = line.split("\t");
					String dist_word = tokens[0];
					String dist_value = tokens[1];
					wordMap.put(dist_word, dist_value);
					line=reader.readLine();
					
				}
					reader.close();
				}
			if(wordMap.isEmpty())
			{
				throw new IOException("My Error: Dictionary not found");
				
			}
		}
			
			public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
			{
				StringTokenizer itr = new StringTokenizer(value.toString());
				while(itr.hasMoreTokens())
				{
					myWord = itr.nextToken().toLowerCase();
					
					if(wordMap.get(myWord)!=null)
					{
						myValue = Integer.parseInt(wordMap.get(myWord));
						
						if(myValue>0)
						{
							myWord="positive";
						}
						
						if(myValue<0)
						{
							myWord="negative";
							myValue = myValue*-1;
						}else
						{
							myWord="positive";
							myValue=0;
						
						}
						word.set(myWord);
						sum_value.set(myValue);
					
						context.write(word, sum_value);	
				
			}
			
			
		}
			}
	}
			public static class SentimentReducer extends Reducer<Text,IntWritable,NullWritable,Text>
			{
				int pos_value=0;
				int neg_value=0;
				double sent_percent = 0.00;
				
				public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException,InterruptedException
				{
					int sum=0;
					int counter=0;
					
					for(IntWritable val: values) 
					{
						sum+=val.get();
					}
					
					counter++;
					
					if(key.toString().equals("positive"))
					{
						pos_value=sum;
					}
					
					if(key.toString().equals("negative"))
					{
						neg_value=sum;
					}
					
				}
			
				
			protected void cleanup(Context context) throws IOException,InterruptedException
			{
				sent_percent = ((pos_value+neg_value)/(pos_value-neg_value))*100;
				String str = "The sentiment analysis value is:" +String.format("%f",sent_percent);
				context.write(NullWritable.get(), new Text(str));
			}
				}
			
			public static void main(String [] args)
					throws IOException,	ClassNotFoundException,InterruptedException
					{
					
					Configuration conf = new Configuration();
					
					Job job = Job.getInstance(conf);
					job.setJarByClass(SentimentAnalysis.class);
					job.setMapperClass(SentimentTokenizerMapper.class);
					job.setReducerClass(SentimentReducer.class);
					job.addCacheFile(new Path(args[0]).toUri());
					//job.setNumReduceTasks(2);
					job.setMapOutputKeyClass(Text.class);
					job.setMapOutputValueClass(IntWritable.class);
					job.setOutputKeyClass(NullWritable.class);
					job.setOutputValueClass(Text.class);
					
					FileInputFormat.addInputPath(job,new Path(args[1]));
					FileOutputFormat.setOutputPath(job,new Path(args[2]));
					
					job.waitForCompletion(true);
					
					}
}
			

