// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.string;

import io.confluent.connect.storage.hive.HiveFactory;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.hdfs3.storage.HdfsStorage;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.format.Format;

public class StringFormat implements Format<Hdfs3SinkConnectorConfig, Path>
{
    private final HdfsStorage storage;
    
    public StringFormat(final HdfsStorage storage) {
        this.storage = storage;
    }
    
    public RecordWriterProvider<Hdfs3SinkConnectorConfig> getRecordWriterProvider() {
        return (RecordWriterProvider<Hdfs3SinkConnectorConfig>)new StringRecordWriterProvider(this.storage);
    }
    
    public SchemaFileReader<Hdfs3SinkConnectorConfig, Path> getSchemaFileReader() {
        return (SchemaFileReader<Hdfs3SinkConnectorConfig, Path>)new StringFileReader();
    }
    
    public HiveFactory getHiveFactory() {
        throw new UnsupportedOperationException("Hive integration is not currently supported with String format");
    }
}
