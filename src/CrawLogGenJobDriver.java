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
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		/*
		 * JobConf conf = new JobConf(getConf(), getClass());
		 * conf.setJobName("Max temperature");
		 * 
		 * 
		 * FileInputFormat.addInputPath(conf, new Path(args[0]));
		 * FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		 * 
		 * conf.setOutputKeyClass(Text.class);
		 * conf.setOutputValueClass(IntWritable.class);
		 * 
		 * conf.setMapperClass(MaxTemperatureMapper.class);
		 * conf.setCombinerClass(MaxTemperatureReducer.class);
		 * conf.setReducerClass(MaxTemperatureReducer.class);
		 * JobClient.runJob(conf);
		 */

		MongoConfigUtil.setOutputURI(getConf(),
				"mongodb://ds037077.mongolab.com:37077/tt_logs.out");

		Job job = new Job(getConf(), "log parser general");
		job.setJarByClass(CrawLogGenJobDriver.class);

		FileInputFormat.addInputPaths(job,
				"s3n://baio-loggly/7868/pub-craw-test/2012/07/31/10.00.raw.gz");

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