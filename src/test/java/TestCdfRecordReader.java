import org.apache.nifi.json.NullSuppression;
import org.apache.nifi.json.WriteJsonResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNameAsAttribute;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.*;

public class TestCdfRecordReader {

    @Test
    public void testCdfRecordReader() throws Exception {
        //InputStream is = new FileInputStream("src/test/resources/cl_jp_pgp_20031001_v52.cdf");
        InputStream is = new FileInputStream("E:\\LOGSA\\cdf_sample\\AH64DAB3\\1009007\\AH64DAB3_1009007_20170101_231155.0_0000045700000014_1009007_2017_01_01_23_11_55_0_XM_0.cdf");
        RecordReader rr = new CdfRecordReader(is);
        final Record record = rr.nextRecord(false, false);
        final RecordSchema schema = rr.getSchema();
        final RecordSet rs = RecordSet.of(schema, record);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final WriteJsonResult writer = new WriteJsonResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, true,
                NullSuppression.NEVER_SUPPRESS, RecordFieldType.DATE.getDefaultFormat(),
                RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat())) {
            writer.write(rs);
        }
        final String output = baos.toString();
        System.out.print(output);

    }
}
