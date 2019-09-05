// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.json;

import io.confluent.connect.storage.hive.HiveFactory;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.format.RecordWriterProvider;
import java.util.Map;
import java.util.HashMap;
import org.apache.kafka.connect.json.JsonConverter;
import io.confluent.connect.hdfs3.storage.HdfsStorage;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.format.Format;

public class JsonFormat implements Format<Hdfs3SinkConnectorConfig, Path>
{
    private final HdfsStorage storage;
    private final JsonConverter converter;
    
    public JsonFormat(final HdfsStorage storage) {
        this.storage = storage;
        this.converter = new JsonConverter();
        final Map<String, Object> converterConfig = new HashMap<String, Object>();
        converterConfig.put("schemas.enable", "false");
        converterConfig.put("schemas.cache.size", String.valueOf(storage.conf().get(Hdfs3SinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG)));
        converter.configure(converterConfig, false);
    }
    
    public RecordWriterProvider<Hdfs3SinkConnectorConfig> getRecordWriterProvider() {
        return (RecordWriterProvider<Hdfs3SinkConnectorConfig>)new JsonRecordWriterProvider(this.storage, this.converter);
    }
    
    public SchemaFileReader<Hdfs3SinkConnectorConfig, Path> getSchemaFileReader() {
        return (SchemaFileReader<Hdfs3SinkConnectorConfig, Path>)new JsonFileReader();
    }
    
    public HiveFactory getHiveFactory() {
        throw new UnsupportedOperationException("Hive integration is not currently supported with JSON format");
    }
}
