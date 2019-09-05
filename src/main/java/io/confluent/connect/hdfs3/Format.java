// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3;

import io.confluent.connect.storage.hive.HiveUtil;
import io.confluent.connect.storage.hive.HiveMetaStore;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.avro.AvroData;

@Deprecated
public interface Format
{
    RecordWriterProvider getRecordWriterProvider();
    
    SchemaFileReader<Hdfs3SinkConnectorConfig, Path> getSchemaFileReader(final AvroData p0);
    
    HiveUtil getHiveUtil(final Hdfs3SinkConnectorConfig p0, final HiveMetaStore p1);
}
