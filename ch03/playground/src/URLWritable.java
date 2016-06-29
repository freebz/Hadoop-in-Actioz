import java.io.*;
import java.net.*;

import org.apache.hadoop.io.*;

public class URLWritable implements Writable {

	protected URL url;

	public URLWritable() { }

	public URLWritable(URL url) {
		this.url = url;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(url.toString());
	}

	public void readFields(DataInput in) throws IOException {
		url = new URL(in.readUTF());
	}

	public void set(String s) throws MalformedURLException {
		url = new URL(s);
	}
}
