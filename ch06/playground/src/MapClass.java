import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MapClass extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text> {

	static enum ClaimsCounters { MISSING, QUOTED };

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {

		String fields[] = value.toString().split(",", -20);
		String country = fields[4];
		String numClaims = fields[8];

		if (numClaims.length() == 0) {
			reporter.incrCounter(ClaimsCounters.MISSING, 1);
		} else if (numClaims.startsWith("\"")) {
			reporter.incrCounter(ClaimsCounters.QUOTED, 1);
		} else {
			output.collect(new Text(country), new Text(numClaims + ",1"));
		}
	}
}
