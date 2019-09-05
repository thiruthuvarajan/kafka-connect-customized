// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.string;

import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.hdfs3.storage.HdfsStorage;
import org.slf4j.Logger;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class StringRecordWriterProvider implements RecordWriterProvider<Hdfs3SinkConnectorConfig>
{
    private static final Logger log;
    private static final String EXTENSION = ".txt";
    private static final int WRITER_BUFFER_SIZE = 131072;
    private final HdfsStorage storage;
    
    StringRecordWriterProvider(final HdfsStorage storage) {
        this.storage = storage;
    }
    
    public String getExtension() {
        return EXTENSION;
    }
    
    public RecordWriter getRecordWriter(final Hdfs3SinkConnectorConfig conf, final String filename) {
        try {
            return (RecordWriter)new RecordWriter() {
                final Path path = new Path(filename);
                final OutputStream out = this.path.getFileSystem(conf.getHadoopConfiguration()).create(this.path);
                final OutputStreamWriter streamWriter = new OutputStreamWriter(this.out, Charset.defaultCharset());
                final BufferedWriter writer = new BufferedWriter(this.streamWriter, WRITER_BUFFER_SIZE);
                
                public void write(final SinkRecord record) {
                    StringRecordWriterProvider.log.trace("Sink record: {}", (Object)record.toString());
                    try {
                        final String value = (String)record.value();
                        this.writer.write(value);
                        this.writer.newLine();
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
        log = LoggerFactory.getLogger(StringRecordWriterProvider.class);
    }
}
