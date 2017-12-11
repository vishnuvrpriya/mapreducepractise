import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SpeedReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	
	
	
	
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		
		float Offence;
		float maxSpeedValue = 65;
		float total = 0;
		float OffencePercent;
		float OffenceCount=0;
		
		for (FloatWritable value : values) {
			
			Offence = value.get();
			
			if(Offence > maxSpeedValue)
				
				OffenceCount++;
			
			}
		
			total++;
			
			OffencePercent = (OffenceCount/total)*100;
		
		
		context.write(key, new FloatWritable(OffencePercent));
	}	
}