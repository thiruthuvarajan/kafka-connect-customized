// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.parquet;

import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.hive.HiveUtil;
import io.confluent.connect.storage.hive.HiveMetaStore;
import org.apache.kafka.common.config.AbstractConfig;
import io.confluent.connect.storage.hive.HiveFactory;

public class ParquetHiveFactory implements HiveFactory
{
	 @Override
    public HiveUtil createHiveUtil(final AbstractConfig config, final HiveMetaStore hiveMetaStore) {
        return this.createHiveUtil((Hdfs3SinkConnectorConfig)config, hiveMetaStore);
    }
    
    @Deprecated
    public HiveUtil createHiveUtil(final Hdfs3SinkConnectorConfig config, final HiveMetaStore hiveMetaStore) {
        return new ParquetHiveUtil(config, hiveMetaStore);
    }
}
