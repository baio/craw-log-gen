import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class CrawLogGenJobDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
						
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <input> <output> <craw_id>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		MongoConfigUtil.setOutputURI(getConf(), args[1]);		
		getConf().set("craw_id", args[2]);
		
		Job job = new Job(getConf(), "log parser general");
		job.setJarByClass(CrawLogGenJobDriver.class);

		FileInputFormat.addInputPaths(job,args[0]);
				
		job.setMapperClass(LogMapper.class);
		job.setReducerClass(LogReducer.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(MongoOutputFormat.class);
		job.setOutputKeyClass(BSONWritable.class);
		job.setOutputValueClass(BSONWritable.class);
		

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CrawLogGenJobDriver(), args);
		System.exit(exitCode);
	}
}