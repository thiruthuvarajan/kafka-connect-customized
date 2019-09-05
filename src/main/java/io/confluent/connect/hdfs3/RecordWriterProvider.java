// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3;

import java.io.IOException;
import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.hadoop.conf.Configuration;
@Deprecated
public interface RecordWriterProvider
{
    String getExtension();
    
    RecordWriter<SinkRecord> getRecordWriter(final Configuration p0, final String p1, final SinkRecord p2, final AvroData p3) throws IOException;
}
