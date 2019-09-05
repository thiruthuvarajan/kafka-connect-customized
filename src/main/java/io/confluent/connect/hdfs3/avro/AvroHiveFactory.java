// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.avro;

import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.hive.HiveUtil;
import io.confluent.connect.storage.hive.HiveMetaStore;
import org.apache.kafka.common.config.AbstractConfig;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.hive.HiveFactory;

public class AvroHiveFactory implements HiveFactory
{
    private final AvroData avroData;
    
    public AvroHiveFactory(final AvroData avroData) {
        this.avroData = avroData;
    }
    
    public HiveUtil createHiveUtil(final AbstractConfig conf, final HiveMetaStore hiveMetaStore) {
        return this.createHiveUtil((Hdfs3SinkConnectorConfig)conf, hiveMetaStore);
    }
    
    @Deprecated
    public HiveUtil createHiveUtil(final Hdfs3SinkConnectorConfig conf, final HiveMetaStore hiveMetaStore) {
        return new AvroHiveUtil(conf, this.avroData, hiveMetaStore);
    }
}
