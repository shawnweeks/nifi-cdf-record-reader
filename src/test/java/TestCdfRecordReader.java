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

import us.weeksconsulting.nifi.cdf.record.reader.CdfRecordReader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class TestCdfRecordReader {
	Set<String> dataTypes = new HashSet<>();

	@Test
	public void testCdfRecordReader() throws Exception {
		try (Stream<Path> paths = Files.walk(Paths.get("E:\\LOGSA\\nasa_cdf_sample"))) {
			paths.filter(Files::isRegularFile).forEach(path -> {
				if (path.getFileName().toString().toLowerCase().endsWith(".cdf")) {
					System.out.println("Processing " + path.getFileName());
					try (InputStream is = new FileInputStream(path.toFile());
							RecordReader rr = new CdfRecordReader(is, dataTypes);) {
						final Record record = rr.nextRecord(false, false);
						final RecordSchema schema = rr.getSchema();
						final RecordSet rs = RecordSet.of(schema, record);
						final ByteArrayOutputStream baos = new ByteArrayOutputStream();

						try (final WriteJsonResult writer = new WriteJsonResult(Mockito.mock(ComponentLog.class),
								schema, new SchemaNameAsAttribute(), baos, true, NullSuppression.NEVER_SUPPRESS,
								RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(),
								RecordFieldType.TIMESTAMP.getDefaultFormat())) {
							writer.write(rs);
						}
    	          final String output = baos.toString();
    	          System.out.print(output);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					System.out.println("Completed " + path.getFileName());
				}
			});
		}
		System.out.println(dataTypes);
	}
}
