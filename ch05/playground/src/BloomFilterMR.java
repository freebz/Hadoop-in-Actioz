import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

public class BloomFilterMR extends Configured implements Tool {

	public static class MapClass extends MapReduceBase
		implements Mapper<Text, Text, Text, BloomFilter<String>> {

		BloomFilter<String> bf = new BloomFilter<String>();
		OutputCollector<Text, BloomFilter<String>> oc = null;

		public void map(Text key, Text value,
				OutputCollector<Text, BloomFilter<String>> output,
				Reporter reporter) throws IOException {
			if (oc == null) oc = output;

			bf.add(key.toString());
		}
	}

	public static class Reduce extends MapReduceBase
		implements Reducer<Text, BloomFilter<String>, Text, Text> {

		JobConf job = null;
		BloomFilter<String> bf = new BloomFilter<String>();

		public void configure(JobConf job) {
			this.job = job;
		}

		public void reduce(Text key, Iterator<BloomFilter<String>> values,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			while (values.hasNext()) {
				bf.union((BloomFilter<String>)values.next());
			}
		}

		public void close() throws IOException {
			Path file = new Path(job.get("mapred.output.dir") + "/bloomfilter");
			FSDataOutputStream out = file.getFileSystem(job).create(file);
			bf.write(out);
			out.close();
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, BloomFilterMR.class);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJobName("Bloom Filter");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);

		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(NullOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BloomFilter.class);
		job.set("key.value.separator.in.input.line", ",");

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new BloomFilterMR(),
				args);

		System.exit(res);
	}
}

