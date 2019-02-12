package us.weeksconsulting.nifi.cdf.record.reader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import uk.ac.bristol.star.cdf.AttributeEntry;
import uk.ac.bristol.star.cdf.CdfContent;
import uk.ac.bristol.star.cdf.CdfReader;
import uk.ac.bristol.star.cdf.DataType;
import uk.ac.bristol.star.cdf.GlobalAttribute;
import uk.ac.bristol.star.cdf.Variable;
import uk.ac.bristol.star.cdf.VariableAttribute;
import uk.ac.bristol.star.cdf.record.SimpleNioBuf;
import uk.ac.bristol.star.cdf.record.VariableDescriptorRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CdfRecordReader implements RecordReader {
	private CdfContent cdfContent;
	private boolean nextRecord = true;
	private Set<String> dataTypes;

	public CdfRecordReader(InputStream in, Set<String> dataTypes) throws IOException {
		// TODO - Remove This.
		this.dataTypes = dataTypes;

		// Setup CDF Reader
		byte[] bytes = IOUtils.toByteArray(in);
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		CdfReader cdfReader = new CdfReader(new SimpleNioBuf(byteBuffer, true, false));
		this.cdfContent = new CdfContent(cdfReader);

	}

	@Override
	public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields)
			throws IOException, MalformedRecordException {
		if (nextRecord) {
			Map<String, Object> recMap = new HashMap<>();

			// TODO - Other CDF Information

			// CDF Global Attributes
			GlobalAttribute[] gAtts = cdfContent.getGlobalAttributes();
			Map<String, Object[]> gAttMap = new HashMap<>();
			for (GlobalAttribute gAtt : gAtts) {
				List<MapRecord> gAttEnts = new ArrayList<>(5);
				for (int i = 0; i < gAtt.getEntries().length; i++) {
					AttributeEntry ent = gAtt.getEntries()[i];
					if (ent != null) {
						Map<String, Object> gAttEntMap = new HashMap<>(3);
						gAttEntMap.put("entry_num", i);
						gAttEntMap.put("entry_type", ent.getDataType().getName());
						gAttEntMap.put("entry_value", ent.toString());
						gAttEnts.add(new MapRecord(CdfRecordSchema.GLOBAL_ATTRIBUTE_ENTRY_SCHEMA, gAttEntMap));
					}
				}
				gAttMap.put(gAtt.getName(), gAttEnts.toArray());
			}
//			recMap.put("global_attributes", gAttMap);

			// CDF Variables
			Variable[] vars = cdfContent.getVariables();
			VariableAttribute[] varAtts = cdfContent.getVariableAttributes();
			Map<String, Object> varMap = new HashMap<>();
			for (Variable var : vars) {
				VariableDescriptorRecord varDesc = var.getDescriptor();
				Map<String, Object> varRecMap = new HashMap<>();
				varRecMap.put("data_type", DataType.getDataType(varDesc.dataType).toString());
				varRecMap.put("num_elements", varDesc.numElems);
				varRecMap.put("dim", varDesc.zNumDims);
				varRecMap.put("dim_sizes", ArrayUtils.toObject(varDesc.zDimSizes));
				varRecMap.put("dim_variances", ArrayUtils.toObject(varDesc.dimVarys));
				// This logic was extracted from the NASA CDFJ and CDF2CDFML Java Code
				// I'm not very good with bitwise operations but we're looking to see if bit 0
				// is set to 1 See
				// https://cdaweb.gsfc.nasa.gov/pub/software/cdf/doc/cdf34/cdf34ifd.pdf page 15
				// for internal documentation.
				varRecMap.put("rec_variance", ((varDesc.flags & 1) != 0));
				varRecMap.put("max_record", varDesc.maxRec);

				Object tmpRecArray = var.createRawValueArray();
				List<Object> varRecRecs = new ArrayList<>();
				for (int i = 0; i < var.getRecordCount(); i++) {
					Map<String, Object> varRecRecMap = new HashMap<>();
					Object shapedRec = var.readShapedRecord(i, true, tmpRecArray);
					dataTypes.add(tmpRecArray.getClass().getTypeName());
//					System.out.println(shapedRec.getClass().getTypeName());
					varRecRecMap.put("rec_num", i);
					// Integer
					if (shapedRec instanceof Integer) {
						varRecRecMap.put("rec_value_int", new Integer[] { (int) shapedRec });
					} else if (shapedRec instanceof int[]) {
						varRecRecMap.put("rec_value", ArrayUtils.toObject((int[]) shapedRec));
					}
					// Short
					else if (shapedRec instanceof Short) {
						varRecRecMap.put("rec_value_short", new Short[] { (short) shapedRec });
					} else if (shapedRec instanceof short[]) {
						varRecRecMap.put("rec_value", ArrayUtils.toObject((short[]) shapedRec));
					}
					// Double
					else if (shapedRec instanceof Double) {
						varRecRecMap.put("rec_value_double", new Double[] { (double) shapedRec });
					} else if (shapedRec instanceof double[]) {
						varRecRecMap.put("rec_value_double", ArrayUtils.toObject((double[]) shapedRec));
//						System.out.println(Arrays.toString((double[]) shapedRec));
					}
					// Float
					else if (shapedRec instanceof Float) {
						varRecRecMap.put("rec_value_float", new Float[] { (float) shapedRec });
					} else if (shapedRec instanceof float[]) {
						varRecRecMap.put("rec_value_float", ArrayUtils.toObject(((float[]) shapedRec)));
					}
					// Byte
					else if (shapedRec instanceof Byte) {
						varRecRecMap.put("rec_value_byte", new Byte[] { (byte) shapedRec });
					} else if (shapedRec instanceof Byte[]) {
						varRecRecMap.put("rec_value_byte", ArrayUtils.toObject(((byte[]) shapedRec)));
					}
					// String
					else if (shapedRec instanceof String) {
						varRecRecMap.put("rec_value_string", new String[] { (String) shapedRec });
					} else if (shapedRec instanceof String[]) {
						varRecRecMap.put("rec_value_string", (String[]) shapedRec);
					}
					// Error if we don't recognize the type.
					else {
						throw new MalformedRecordException("Unexpected Record Type "
								+ shapedRec.getClass().getTypeName() + " for Variable " + var.getName());
					}
//					varRecRecMap.put("rec_value", toStringArray(shapedRec));
					varRecRecs.add(new MapRecord(CdfRecordSchema.VARIABLE_RECORD_SCHEMA, varRecRecMap));
				}
				varRecMap.put("records", varRecRecs.toArray());

				// Variable Attributes
				Map<String, Object> attMap = new HashMap<>();
				for (VariableAttribute varAtt : varAtts) {
					AttributeEntry ent = varAtt.getEntry(var);
					if (ent != null) {
						Map<String, Object> entMap = new HashMap<>();
						entMap.put("entry_type", ent.getDataType().getName());
						entMap.put("entry_value", ent.toString());
						attMap.put(varAtt.getName(), entMap);
					}
				}
				varRecMap.put("attributes", attMap);
				varMap.put(var.getName(), new MapRecord(CdfRecordSchema.VARIABLE_SCHEMA, varRecMap));
			}
			recMap.put("variables", varMap);
			Record record = new MapRecord(CdfRecordSchema.SCHEMA, recMap);
			nextRecord = false;
			return record;
		} else {
			return null;
		}
	}

	@Override
	public RecordSchema getSchema() throws MalformedRecordException {
		return CdfRecordSchema.SCHEMA;
	}

	@Override
	public void close() throws IOException {
	}

	private String[] toStringArray(Object o) throws MalformedRecordException {
		if (o.getClass().isArray()) {
			String[] sArray;
			// Integer
			if (o instanceof int[]) {
				int length = ((int[]) o).length;
				sArray = new String[length];
				for (int i = 0; i < length; i++) {
					sArray[i] = String.valueOf(((int[]) o)[i]);
				}
			}
			// Short
			else if (o instanceof short[]) {
				int length = ((short[]) o).length;
				sArray = new String[length];
				for (int i = 0; i < length; i++) {
					sArray[i] = String.valueOf(((short[]) o)[i]);
				}
			}
			// Double
			else if (o instanceof double[]) {
				int length = ((double[]) o).length;
				sArray = new String[length];
				for (int i = 0; i < length; i++) {
					sArray[i] = String.valueOf(((double[]) o)[i]);
				}
			}
			// Float
			else if (o instanceof float[]) {
				int length = ((float[]) o).length;
				sArray = new String[length];
				for (int i = 0; i < length; i++) {
					sArray[i] = String.valueOf(((float[]) o)[i]);
				}
			}
			// Byte
			else if (o instanceof byte[]) {
				int length = ((byte[]) o).length;
				sArray = new String[length];
				for (int i = 0; i < length; i++) {
					sArray[i] = String.valueOf(((byte[]) o)[i]);
				}
			}
			// String
			else if (o instanceof String[]) {
				sArray = (String[]) o;
			}
			// Error if we don't recognize the type.
			else {
				throw new MalformedRecordException("Unexpected Record Type " + o.getClass().getTypeName());
			}
			return sArray;
		} else {
			return new String[] { String.valueOf(o) };
		}
	}
}
