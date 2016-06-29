import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;

public class ChainJob extends Configured implements Tool {

	public static class MapClass extends MapReduceBase
		implements Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			output.collect(value, key);
		}
	}

	public static class Reduce extends MapReduceBase
		implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {

			String csv = "";
			while (values.hasNext()) {
				if (csv.length() > 0) csv += ",";
				csv += values.next().toString();
			}
			output.collect(key, new Text(csv));
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		JobConf job = new JobConf(conf, ChainJob.class);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		job.setJobName("ChainJob");
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		JobConf map1Conf = new JobConf(false);
		ChainMapper.addMapper(job,
				Map1.class,
				LongWritable.class,
				Text.class,
				Text.class,
				Text.class,
				true,
				map1Conf);

		JobConf map2Conf = new JobConf(false);
		ChainMapper.addMapper(job,
				Map2.class,
				Text.class,
				Text.class,
				LongWritable.class,
				Text.class,
				true,
				map2Conf);

		JobConf reduceConf = new JobConf(false);
		ChainReducer.setReducer(job,
				Reduce.class,
				LongWritable.class,
				Text.class,
				Text.class,
				Text.class,
				true,
				reduceConf);

		JobConf map3Conf = new JobConf(false);
		ChainReducer.addMapper(job,
				Map3.class,
				Text.class,
				Text.class,
				LongWritable.class,
				Text.class,
				true,
				map3Conf);

		JobConf map4Conf = new JobConf(false);
		ChainReducer.addMapper(job,
				Map4.class,
				LongWritable.class,
				Text.class,
				LongWritable.class,
				Text.class,
				true,
				map4Conf);

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ChainJob(), args);

		System.exit(res);
	}
}
