import java.io.*;
import java.sql.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.db.*;

public class EventsDBWritable implements Writable, DBWritable {
	private int id;
	private long timestamp;

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeLong(timestamp);
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		timestamp = in.readLong();
	}

	public void write(PreparedStatement statement) throws SQLException {
		statement.setInt(1, id);
		statement.setLong(2, timestamp);
	}

	public void readFields(ResultSet resultSet) throws SQLException {
		id = resultSet.getInt(1);
		timestamp = resultSet.getLong(2);
	}
}
