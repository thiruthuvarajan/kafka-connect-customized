// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.parquet;

import org.slf4j.LoggerFactory;
import java.io.IOException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.avro.AvroData;
import org.slf4j.Logger;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class ParquetRecordWriterProvider implements RecordWriterProvider<Hdfs3SinkConnectorConfig>
{
    private static final Logger log;
    private static final String EXTENSION = ".parquet";
    private final AvroData avroData;
    
    ParquetRecordWriterProvider(final AvroData avroData) {
        this.avroData = avroData;
    }
    
    public String getExtension() {
        return EXTENSION;
    }
    
    public RecordWriter getRecordWriter(final Hdfs3SinkConnectorConfig conf, final String filename) {
        return (RecordWriter)new RecordWriter() {
            final CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
            final int blockSize = 268435456;
            final int pageSize = 65536;
            final Path path = new Path(filename);
            Schema schema = null;
            ParquetWriter<GenericRecord> writer = null;
            
            public void write(final SinkRecord record) {
                if (this.schema == null) {
                    this.schema = record.valueSchema();
                }
                if (this.writer == null) {
                    try {
                        ParquetRecordWriterProvider.log.info("Opening record writer for: {}", (Object)filename);
                        final org.apache.avro.Schema avroSchema = ParquetRecordWriterProvider.this.avroData.fromConnectSchema(this.schema);
                        this.writer = AvroParquetWriter.<GenericRecord>builder(path)
                        			.withSchema(avroSchema)
                        			.withCompressionCodec(compressionCodecName)
                        			.withRowGroupSize(blockSize)
									.withPageSize(pageSize)
									.withDictionaryEncoding(true)
									.withConf(conf.getHadoopConfiguration())
									.withWriteMode(ParquetFileWriter.Mode.OVERWRITE).build();
                        ParquetRecordWriterProvider.log.debug("Opened record writer for: {}", (Object)filename);
                    }
                    catch (IOException e) {
                        ParquetRecordWriterProvider.log.warn("Error creating {} for file '{}', {}, and schema {}: ", new Object[] { AvroParquetWriter.class.getSimpleName(), filename, this.compressionCodecName, this.schema, e });
                        throw new ConnectException((Throwable)e);
                    }
                }
                ParquetRecordWriterProvider.log.trace("Sink record: {}", (Object)record);
                final Object value = ParquetRecordWriterProvider.this.avroData.fromConnectData(record.valueSchema(), record.value());
                try {
                    this.writer.write((GenericRecord) value);
                }
                catch (IOException e2) {
                    throw new ConnectException((Throwable)e2);
                }
            }
            
            public void close() {
                if (this.writer != null) {
                    try {
                        writer.close();
                    }
                    catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }
            }
            
            public void commit() {
            }
        };
    }
    
    static {
        log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
    }
}
