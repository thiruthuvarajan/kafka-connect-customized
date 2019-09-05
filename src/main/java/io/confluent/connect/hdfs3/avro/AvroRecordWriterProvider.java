// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.avro;

import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.connect.errors.DataException;
import io.confluent.kafka.serializers.NonRecordContainer;
import java.io.IOException;
import org.apache.kafka.connect.errors.ConnectException;
import java.io.OutputStream;
import org.apache.avro.file.CodecFactory;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.avro.file.DataFileWriter;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.avro.AvroData;
import org.slf4j.Logger;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class AvroRecordWriterProvider implements RecordWriterProvider<Hdfs3SinkConnectorConfig>
{
    private static final Logger log;
    private static final String EXTENSION = ".avro";
    private final AvroData avroData;
    
    AvroRecordWriterProvider(final AvroData avroData) {
        this.avroData = avroData;
    }
    
    public String getExtension() {
        return EXTENSION;
    }
    
    public RecordWriter getRecordWriter(final Hdfs3SinkConnectorConfig conf, final String filename) {
        return (RecordWriter)new RecordWriter() {
			final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
            final Path path = new Path(filename);
            Schema schema = null;
            
            public void write(final SinkRecord record) {
                if (this.schema == null) {
                    this.schema = record.valueSchema();
                    try {
                        AvroRecordWriterProvider.log.info("Opening record writer for: {}", (Object)filename);
                        final FSDataOutputStream out = this.path.getFileSystem(conf.getHadoopConfiguration()).create(this.path);
                        final org.apache.avro.Schema avroSchema = AvroRecordWriterProvider.this.avroData.fromConnectSchema(this.schema);
                        this.writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
                        this.writer.create(avroSchema, (OutputStream)out);
                    }
                    catch (IOException e) {
                        throw new ConnectException((Throwable)e);
                    }
                }
                AvroRecordWriterProvider.log.trace("Sink record: {}", (Object)record);
                final Object value = AvroRecordWriterProvider.this.avroData.fromConnectData(this.schema, record.value());
                try {
                    if (value instanceof NonRecordContainer) {
                        this.writer.append(((NonRecordContainer)value).getValue());
                    }
                    else {
                        this.writer.append(value);
                    }
                }
                catch (IOException e2) {
                    throw new DataException((Throwable)e2);
                }
            }
            
            public void close() {
                try {
                    this.writer.close();
                }
                catch (IOException e) {
                    throw new DataException((Throwable)e);
                }
            }
            
            public void commit() {
            }
        };
    }
    
    static {
        log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
    }
}
