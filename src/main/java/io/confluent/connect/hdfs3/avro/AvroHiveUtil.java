// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.avro;

import java.util.List;
import io.confluent.connect.storage.hive.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.HiveMetaStoreException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.common.config.AbstractConfig;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.hive.HiveUtil;

public class AvroHiveUtil extends HiveUtil
{
    private static final String AVRO_SERDE = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
    private static final String AVRO_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
    private static final String AVRO_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
    private static final String AVRO_SCHEMA_LITERAL = "avro.schema.literal";
    private final AvroData avroData;
    private final String topicsDir;
    
    public AvroHiveUtil(final Hdfs3SinkConnectorConfig conf, final AvroData avroData, final HiveMetaStore hiveMetaStore) {
        super((AbstractConfig)conf, hiveMetaStore);
        this.avroData = avroData;
        this.topicsDir = conf.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    }
    
    @Override
    public void createTable(final String database, final String tableName, final Schema schema, final Partitioner<FieldSchema> partitioner) throws HiveMetaStoreException {
        final Table table = this.constructAvroTable(database, tableName, schema, partitioner);
        this.hiveMetaStore.createTable(table);
    }
    
    @Override
    public void alterSchema(final String database, final String tableName, final Schema schema) throws HiveMetaStoreException {
        final Table table = this.hiveMetaStore.getTable(database, tableName);
        table.getParameters().put(AVRO_SCHEMA_LITERAL, this.avroData.fromConnectSchema(schema).toString());
        this.hiveMetaStore.alterTable(table);
    }
    
    private Table constructAvroTable(final String database, final String tableName, final Schema schema, final Partitioner<FieldSchema> partitioner) throws HiveMetaStoreException {
        final Table table = this.newTable(database, tableName);
        table.setTableType(TableType.EXTERNAL_TABLE);
        table.getParameters().put("EXTERNAL", "TRUE");
        final String tablePath = this.hiveDirectoryName(this.url, this.topicsDir, tableName);
        table.setDataLocation(new Path(tablePath));
        table.setSerializationLib(AVRO_SERDE);
        try {
            table.setInputFormatClass(AVRO_INPUT_FORMAT);
            table.setOutputFormatClass(AVRO_OUTPUT_FORMAT);
        }
        catch (HiveException e) {
            throw new HiveMetaStoreException("Cannot find input/output format:", (Throwable)e);
        }
        final List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
        table.setFields(columns);
        table.setPartCols(partitioner.partitionFields());
        table.getParameters().put(AVRO_SCHEMA_LITERAL, this.avroData.fromConnectSchema(schema).toString());
        return table;
    }
}
