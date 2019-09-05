// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.avro;

import io.confluent.connect.storage.hive.HiveFactory;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.hdfs3.storage.HdfsStorage;
import io.confluent.connect.avro.AvroData;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.format.Format;

public class AvroFormat implements Format<Hdfs3SinkConnectorConfig, Path>
{
    private final AvroData avroData;
    
    public AvroFormat(final HdfsStorage storage) {
        this.avroData = new AvroData((int)storage.conf().getInt(Hdfs3SinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG));
    }
    
    public RecordWriterProvider<Hdfs3SinkConnectorConfig> getRecordWriterProvider() {
        return (RecordWriterProvider<Hdfs3SinkConnectorConfig>)new AvroRecordWriterProvider(this.avroData);
    }
    
    public SchemaFileReader<Hdfs3SinkConnectorConfig, Path> getSchemaFileReader() {
        return (SchemaFileReader<Hdfs3SinkConnectorConfig, Path>)new AvroFileReader(this.avroData);
    }
    
    public HiveFactory getHiveFactory() {
        return (HiveFactory)new AvroHiveFactory(this.avroData);
    }
}
