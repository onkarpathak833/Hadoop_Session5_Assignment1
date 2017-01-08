package Session05Assignment01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task02Mapper  {
	
	public static class Mapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			String []lineValues = line.split("\\|");
			context.write(new Text(lineValues[0]), new IntWritable(1));
		}
		
	}

}
