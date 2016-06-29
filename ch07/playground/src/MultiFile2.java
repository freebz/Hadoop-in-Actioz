import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

public class MultiFile2 extends Configured implements Tool {

	public static class MapClass extends MapReduceBase
		implements Mapper<LongWritable, Text, NullWritable, Text> {

		private MultipleOutputs mos;
		private OutputCollector<NullWritable, Text> collector;

		public void configure(JobConf conf) {
			mos = new MultipleOutputs(conf);
		}

		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, Text> output,
				Reporter reporter) throws IOException {

			String[] arr = value.toString().split(",", -1);
			String chrono = arr[0] + "," + arr[1] + "," + arr[2];
			String geo = arr[0] + "," + arr[4] + "," + arr[5];

			collector = mos.getCollector("chrono", reporter);
			collector.collect(NullWritable.get(), new Text(chrono));
			collector = mos.getCollector("geo", reporter);
			collector.collect(NullWritable.get(), new Text(geo));
		}

		public void close() throws IOException {
			mos.close();
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		JobConf job = new JobConf(conf, MultiFile2.class);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJobName("MultiFile");
		job.setMapperClass(MapClass.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		MultipleOutputs.addNamedOutput(job,
				"chrono",
				TextOutputFormat.class,
				NullWritable.class,
				Text.class);
		MultipleOutputs.addNamedOutput(job,
				"geo",
				TextOutputFormat.class,
				NullWritable.class,
				Text.class);

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MultiFile2(),
				args);

		System.exit(res);
	}
}
