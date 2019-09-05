// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.json;

import java.nio.charset.StandardCharsets;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import java.io.IOException;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.OutputStream;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.storage.format.RecordWriter;
import org.apache.kafka.connect.json.JsonConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.hdfs3.storage.HdfsStorage;
import org.slf4j.Logger;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class JsonRecordWriterProvider implements RecordWriterProvider<Hdfs3SinkConnectorConfig>
{
    private static final Logger log;
    private static final String EXTENSION = ".json";
    private static final String LINE_SEPARATOR;
    private static final byte[] LINE_SEPARATOR_BYTES;
    private final HdfsStorage storage;
    private final ObjectMapper mapper;
    private final JsonConverter converter;
    
    JsonRecordWriterProvider(final HdfsStorage storage, final JsonConverter converter) {
        this.storage = storage;
        this.mapper = new ObjectMapper();
        this.converter = converter;
    }
    
    @Override
    public String getExtension() {
        return EXTENSION;
    }
    
    @Override
    public RecordWriter getRecordWriter(final Hdfs3SinkConnectorConfig conf, final String filename) {
        try {
            return (RecordWriter)new RecordWriter() {
                final Path path = new Path(filename);
                final OutputStream out = this.path.getFileSystem(conf.getHadoopConfiguration()).create(this.path);
                final JsonGenerator writer = JsonRecordWriterProvider.this.mapper.getFactory().createGenerator(this.out).setRootValueSeparator((SerializableString)null);
                
                public void write(final SinkRecord record) {
                    JsonRecordWriterProvider.log.trace("Sink record: {}", (Object)record.toString());
                    try {
                        final Object value = record.value();
                        if (value instanceof Struct) {
                            final byte[] rawJson = JsonRecordWriterProvider.this.converter.fromConnectData(record.topic(), record.valueSchema(), value);
                            this.out.write(rawJson);
                            this.out.write(JsonRecordWriterProvider.LINE_SEPARATOR_BYTES);
                        }
                        else {
                            this.writer.writeObject(value);
                            this.writer.writeRaw(JsonRecordWriterProvider.LINE_SEPARATOR);
                        }
                    }
                    catch (IOException e) {
                        throw new ConnectException((Throwable)e);
                    }
                }
                
                public void commit() {
                }
                
                public void close() {
                    try {
                        this.writer.close();
                    }
                    catch (IOException e) {
                        throw new ConnectException((Throwable)e);
                    }
                }
            };
        }
        catch (IOException e) {
            throw new ConnectException((Throwable)e);
        }
    }
    
    static {
        log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
        LINE_SEPARATOR = System.lineSeparator();
        LINE_SEPARATOR_BYTES = JsonRecordWriterProvider.LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);
    }
}
