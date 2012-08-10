import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;

public class LogReducer extends Reducer<Text, Text, BSONObject, BSONObject> {

	private final static LogFormatter logForamtter = new LogFormatter();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		BSONObject bson = null;

		try {
			bson = logForamtter.AsBSON(key, values);
		} catch (java.text.ParseException e) {
		}

		if (bson != null) {
			context.write(bson, bson);
		}
	}
}
