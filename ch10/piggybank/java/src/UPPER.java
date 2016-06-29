import java.io.*;
import java.util.*;

import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.schema.*;

public class UPPER extends EvalFunc<String>
{
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			String str = (String)input.get(0);
			return str.toUpperCase();
		} catch(Exception e) {
			System.err.println("Failed to process input; error - " +
					e.getMessage());
			return null;
		}
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		funcList.add(new FuncSpec(this.getClass().getName(),
					new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
		return funcList;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(
				new Schema.FieldSchema(
					getSchemaName(this.getClass().getName().toLowerCase(), input),
					DataType.CHARARRAY)
				);
	}
}
