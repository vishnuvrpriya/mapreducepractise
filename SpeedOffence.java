import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class SpeedOffence {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: SpeedOffence <input dir> <output dir>\n");
			System.exit(-1);
		}

		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Speed Offence Computation");

		job.setJarByClass(SpeedOffence.class);

		job.setJobName("Speed Data for Vehicles");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(SpeedMaper.class);
		
		job.setReducerClass(SpeedReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}