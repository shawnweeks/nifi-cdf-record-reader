package us.weeksconsulting.nifi.cdf.record.reader;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class CdfRecordSchema {

	static final RecordSchema SCHEMA;
	static final RecordSchema GLOBAL_ATTRIBUTE_ENTRY_SCHEMA;
	static final RecordSchema VARIABLE_SCHEMA;
	static final RecordSchema VARIABLE_ATTRIBUTE_SCHEMA;
	static final RecordSchema VARIABLE_RECORD_SCHEMA;

	static {
		// Setup CDF Record Schema
		List<RecordField> fields = new ArrayList<>();

		// Setup CDF Global Attribute Record Schema
		fields.clear();
		fields.add(new RecordField("entry_num", RecordFieldType.INT.getDataType()));
		fields.add(new RecordField("entry_type", RecordFieldType.STRING.getDataType()));
		fields.add(new RecordField("entry_value", RecordFieldType.STRING.getDataType()));
		GLOBAL_ATTRIBUTE_ENTRY_SCHEMA = new SimpleRecordSchema(fields);

		// Setup CDF Variable Attribute Record Schema
		fields.clear();
		fields.add(new RecordField("entry_type", RecordFieldType.STRING.getDataType()));
		fields.add(new RecordField("entry_value", RecordFieldType.STRING.getDataType()));
		VARIABLE_ATTRIBUTE_SCHEMA = new SimpleRecordSchema(fields);

		// Setup CDF Variable Record Schema
		fields.clear();
		fields.add(new RecordField("rec_num", RecordFieldType.STRING.getDataType()));
//		fields.add(new RecordField("rec_delim", RecordFieldType.STRING.getDataType()));
		fields.add(new RecordField("rec_value_string",
				RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
		fields.add(new RecordField("rec_value_double",
				RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.DOUBLE.getDataType())));
		fields.add(new RecordField("rec_value_short",
				RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.SHORT.getDataType())));
		fields.add(new RecordField("rec_value_int",
				RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.SHORT.getDataType())));
		fields.add(new RecordField("rec_value_float",
				RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.FLOAT.getDataType())));
		fields.add(new RecordField("rec_value_byte",
				RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
		VARIABLE_RECORD_SCHEMA = new SimpleRecordSchema(fields);

		// Setup CDF Variable Record Schema
		fields.clear();
		fields.add(new RecordField("data_type", RecordFieldType.STRING.getDataType()));
		fields.add(new RecordField("num_elements", RecordFieldType.INT.getDataType()));
		fields.add(new RecordField("dim", RecordFieldType.INT.getDataType()));
		fields.add(new RecordField("dim_sizes",
				RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));
		fields.add(new RecordField("rec_variance", RecordFieldType.BOOLEAN.getDataType()));
		fields.add(new RecordField("dim_variances",
				RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
		fields.add(new RecordField("max_record", RecordFieldType.INT.getDataType()));
		fields.add(new RecordField("records", RecordFieldType.ARRAY
				.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(VARIABLE_RECORD_SCHEMA))));
		fields.add(new RecordField("attributes", RecordFieldType.MAP
				.getMapDataType(RecordFieldType.RECORD.getRecordDataType(VARIABLE_ATTRIBUTE_SCHEMA))));
		VARIABLE_SCHEMA = new SimpleRecordSchema(fields);

		// Setup CDF Output Record Schema
		fields.clear();
		fields.add(new RecordField("global_attributes", RecordFieldType.MAP.getMapDataType(RecordFieldType.ARRAY
				.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(GLOBAL_ATTRIBUTE_ENTRY_SCHEMA)))));
		fields.add(new RecordField("variables",
				RecordFieldType.MAP.getMapDataType(RecordFieldType.RECORD.getRecordDataType(VARIABLE_SCHEMA))));

		SCHEMA = new SimpleRecordSchema(fields);
	}

}
