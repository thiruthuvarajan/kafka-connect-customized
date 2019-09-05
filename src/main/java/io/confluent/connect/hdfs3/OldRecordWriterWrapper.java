// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3;

import java.io.IOException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import io.confluent.connect.storage.format.RecordWriter;

public class OldRecordWriterWrapper implements RecordWriter
{
    private final io.confluent.connect.hdfs3.RecordWriter<SinkRecord> oldWriter;
    
    public OldRecordWriterWrapper(final io.confluent.connect.hdfs3.RecordWriter<SinkRecord> oldWriter) {
        this.oldWriter = oldWriter;
    }
    
    public void write(final SinkRecord sinkRecord) {
        try {
            this.oldWriter.write(sinkRecord);
        }
        catch (IOException e) {
            throw new ConnectException("Failed to write a record to " + this.oldWriter, (Throwable)e);
        }
    }
    
    public void close() {
        try {
            this.oldWriter.close();
        }
        catch (IOException e) {
            throw new ConnectException("Failed to close " + this.oldWriter, (Throwable)e);
        }
    }
    
    public void commit() {
    }
}
