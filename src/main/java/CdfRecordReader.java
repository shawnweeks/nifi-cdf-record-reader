import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import uk.ac.bristol.star.cdf.*;
import uk.ac.bristol.star.cdf.DataType;
import uk.ac.bristol.star.cdf.record.SimpleNioBuf;
import uk.ac.bristol.star.cdf.record.VariableDescriptorRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;

public class CdfRecordReader implements RecordReader {
    private CdfContent cdfContent;
    private RecordSchema attributeSchema;
    private RecordSchema variableSchema;
    private RecordSchema schema;
    private boolean nextRecord = true;

    public CdfRecordReader(InputStream in) throws IOException {
        // Setup CDF Reader
        byte[] bytes = IOUtils.toByteArray(in);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        CdfReader cdfReader = new CdfReader(new SimpleNioBuf(byteBuffer, true, false));
        this.cdfContent = new CdfContent(cdfReader);
        // Setup CDF Record Schema
        List<RecordField> fields;

        // Setup CDF Attribute Record Schema
        fields = new ArrayList<>();
        fields.add(new RecordField("attType", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("attValue", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("attItemCount", RecordFieldType.INT.getDataType()));
        this.attributeSchema = new SimpleRecordSchema(fields);

        // Setup CDF Variable Record Schema
        fields = new ArrayList<>();
        fields.add(new RecordField("dataType", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("records", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.DOUBLE.getDataType()))));
        fields.add(new RecordField("attributes", RecordFieldType.MAP.getMapDataType(RecordFieldType.RECORD.getRecordDataType(attributeSchema))));
        this.variableSchema = new SimpleRecordSchema(fields);

        // Setup CDF Output Record Schema
        fields = new ArrayList<>();
        fields.add(new RecordField("rowMajor", RecordFieldType.BOOLEAN.getDataType()));
        fields.add(new RecordField("rDimSizes", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));
        fields.add(new RecordField("leapSecondLastUpdated", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("gAttributes", RecordFieldType.MAP.getMapDataType(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(attributeSchema)))));
        fields.add(new RecordField("variables", RecordFieldType.MAP.getMapDataType(RecordFieldType.RECORD.getRecordDataType(variableSchema))));
        this.schema = new SimpleRecordSchema(fields);
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (nextRecord) {
            Map<String, Object> mapRecord = new HashMap<>();

            // CDF Info
            CdfInfo cdfInfo = cdfContent.getCdfInfo();
            mapRecord.put("rowMajor", cdfInfo.getRowMajor());
            mapRecord.put("rDimSizes", ArrayUtils.toObject(cdfInfo.getRDimSizes()));
            mapRecord.put("leapSecondLastUpdated", cdfInfo.getLeapSecondLastUpdated());

            // CDF Global Attributes
            GlobalAttribute[] gAtts = cdfContent.getGlobalAttributes();
            Map<String, Object[]> gAttMap = new HashMap<>();
            for (GlobalAttribute gAtt : gAtts) {
                List<MapRecord> ents = new ArrayList<>();
                for (AttributeEntry ent : gAtt.getEntries()) {
                    Map<String, Object> entMap = new HashMap<>();
                    entMap.put("attType", ent.getDataType().getName());
                    entMap.put("attValue", ent.toString().substring(0, Math.min(ent.toString().length(), 10)));
                    entMap.put("attItemCount", ent.getItemCount());
                    ents.add(new MapRecord(attributeSchema, entMap));
                }
                gAttMap.put(gAtt.getName(), ents.toArray());
            }
            //mapRecord.put("gAttributes", gAttMap);

            // CDF Variables
            Variable[] vars = cdfContent.getVariables();
            VariableAttribute[] vAtts = cdfContent.getVariableAttributes();
            Map<String, Object> varMap = new HashMap<>();
            for (Variable var : vars) {
                Map<String, Object> varRecord = new HashMap<>();
                VariableDescriptorRecord varDesc = var.getDescriptor();
                varRecord.put("dataType", DataType.getDataType(varDesc.dataType).toString());
                varRecord.put("name", varDesc.name);

                List<Object> records = new ArrayList<>();
                for (int i = 0; i < var.getRecordCount(); i++) {
                    Object record = var.createRawValueArray();
                    var.readRawRecord(i, record);
                    if (record instanceof double[]) {
                        records.add(ArrayUtils.toObject(Arrays.copyOf((double[]) record, Math.min(((double[]) record).length, 4))));
                    }
                }
                varRecord.put("records", records.toArray());

                // Variable Attributes
                Map<String, Object> attMap = new HashMap<>();
                for (VariableAttribute vAtt : vAtts) {
                    AttributeEntry ent = vAtt.getEntry(var);
                    if (ent != null) {
                        Map<String, Object> entMap = new HashMap<>();
                        entMap.put("attType", ent.getDataType().getName());
                        entMap.put("attValue", ent.toString().substring(0, Math.min(ent.toString().length(), 10)));
                        entMap.put("attItemCount", ent.getItemCount());
                        attMap.put(vAtt.getName(), entMap);
                    }
                }
//                varRecord.put("attributes", attMap);
                varMap.put(var.getName(), new MapRecord(variableSchema, varRecord));
            }
            mapRecord.put("variables", varMap);
            Record record = new MapRecord(schema, mapRecord);
            nextRecord = false;
            return record;
        } else {
            return null;
        }
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return this.schema;
    }

    @Override
    public void close() throws IOException {

    }

    private Object[] getAttributes() {
        return null;
    }
}
