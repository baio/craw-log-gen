import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private final static LogParser logParser = new LogParser();
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		KeyValue<String, String> kv = logParser.ParseLine(value.toString());

		if (kv != null) {
			context.write(new Text(kv.Key.toString()), new Text(kv.Val));
		}
	}
}
