// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.parquet;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import java.util.List;
import io.confluent.connect.storage.hive.HiveSchemaConverter;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.HiveMetaStoreException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.common.config.AbstractConfig;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.hive.HiveUtil;

public class ParquetHiveUtil extends HiveUtil
{
    private final String topicsDir;
    
    public ParquetHiveUtil(final Hdfs3SinkConnectorConfig conf, final HiveMetaStore hiveMetaStore) {
        super((AbstractConfig)conf, hiveMetaStore);
        this.topicsDir = conf.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    }
    
    public void createTable(String database, String tableName, Schema schema, Partitioner<FieldSchema> partitioner) throws HiveMetaStoreException {
        final Table table = this.constructParquetTable(database, tableName, schema, partitioner);
        this.hiveMetaStore.createTable(table);
    }
    
    public void alterSchema(String database, String tableName, Schema schema) {
        Table table = hiveMetaStore.getTable(database, tableName);
        final List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
        table.setFields(columns);
        hiveMetaStore.alterTable(table);
    }
    
    private Table constructParquetTable(String database, String tableName, Schema schema, Partitioner<FieldSchema> partitioner) throws HiveMetaStoreException {
        Table table = this.newTable(database, tableName);
        table.setTableType(TableType.EXTERNAL_TABLE);
        table.getParameters().put("EXTERNAL", "TRUE");
        String tablePath = hiveDirectoryName(this.url, this.topicsDir, tableName);
        table.setDataLocation(new Path(tablePath));
        table.setSerializationLib(this.getHiveParquetSerde());
        try {
            table.setInputFormatClass(this.getHiveParquetInputFormat());
            table.setOutputFormatClass(this.getHiveParquetOutputFormat());
        }
        catch (HiveException e) {
            throw new HiveMetaStoreException("Cannot find input/output format:", (Throwable)e);
        }
        final List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
        table.setFields(columns);
        table.setPartCols(partitioner.partitionFields());
        return table;
    }
    
    private String getHiveParquetInputFormat() {
        final String newClass = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
        final String oldClass = "parquet.hive.DeprecatedParquetInputFormat";
        try {
            Class.forName(newClass);
            return newClass;
        }
        catch (ClassNotFoundException ex) {
            return oldClass;
        }
    }
    
    private String getHiveParquetOutputFormat() {
        final String newClass = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
        final String oldClass = "parquet.hive.DeprecatedParquetOutputFormat";
        try {
            Class.forName(newClass);
            return newClass;
        }
        catch (ClassNotFoundException ex) {
            return oldClass;
        }
    }
    
    private String getHiveParquetSerde() {
        final String newClass = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
        final String oldClass = "parquet.hive.serde.ParquetHiveSerDe";
        try {
            Class.forName(newClass);
            return newClass;
        }
        catch (ClassNotFoundException ex) {
            return oldClass;
        }
    }
}
