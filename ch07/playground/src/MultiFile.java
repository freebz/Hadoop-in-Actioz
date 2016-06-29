import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

public class MultiFile extends Configured implements Tool {

	public static class MapClass extends MapReduceBase
		implements Mapper<LongWritable, Text, NullWritable, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, Text> output,
				Reporter reporter) throws IOException {

			output.collect(NullWritable.get(), value);
		}
	}

	public static class PartitionByCountryMTOF
		extends MultipleTextOutputFormat<NullWritable,Text>
	{
		protected String generateFileNameForKeyValue(NullWritable key,
				Text value,
				String filename)
		{
			String[] arr = value.toString().split(",", -1);
			String country = arr[4].substring(1,3);
			return country + "/" + filename;
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		JobConf job = new JobConf(conf, MultiFile.class);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJobName("MultiFile");
		job.setMapperClass(MapClass.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(PartitionByCountryMTOF.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MultiFile(),
				args);

		System.exit(res);
	}
}
