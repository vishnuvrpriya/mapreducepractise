import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SpeedMaper extends Mapper<Text,FloatWritable, Text, FloatWritable> {
	
			
	
	public void map(Text key, FloatWritable value, Context context)
			throws IOException, InterruptedException {
		
		try {
			
			String[] record = value.toString().split(",");
			
			String vehNo = record[0];
			
			
			float offence = Float.valueOf(record[1]);
			
			context.write(new Text(vehNo), new FloatWritable(offence));
			
		} catch (IndexOutOfBoundsException e) {
		} catch (ArithmeticException e1) {
		}
	}
	
}