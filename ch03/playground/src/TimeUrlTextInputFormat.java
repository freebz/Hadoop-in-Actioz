import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TimeUrlTextInputFormat extends FileInputFormat<Text, URLWritable> {

	public RecordReader<Text, URLWritable> getRecordReader(
			InputSplit input, JobConf job, Reporter reporter)
		throws IOException {

			return new TimeUrlLineRecordReader(job, (FileSplit)input);
		}
}
