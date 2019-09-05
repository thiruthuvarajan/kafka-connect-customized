// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3;

import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.HashMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.connect.sink.SinkRecord;
import java.util.Collection;
import org.apache.kafka.common.TopicPartition;
import java.util.Set;
import org.apache.kafka.connect.errors.ConnectException;
import org.joda.time.DateTimeZone;
import org.apache.kafka.common.config.ConfigException;

import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import java.util.Map;
import io.confluent.connect.utils.Version;
import io.confluent.connect.avro.AvroData;
import org.slf4j.Logger;
import org.apache.kafka.connect.sink.SinkTask;

public class Hdfs3SinkTask extends SinkTask
{
    private static final Logger log;
    private DataWriter hdfsWriter;
    private AvroData avroData;
    
    public String version() {
        return Version.forClass(this.getClass());
    }
    
    public void start(final Map<String, String> props) {
        Set<TopicPartition> assignment = context.assignment();
        try {
            final Hdfs3SinkConnectorConfig connectorConfig = new Hdfs3SinkConnectorConfig(props);
            final boolean hiveIntegration = connectorConfig.getBoolean(HiveConfig.HIVE_INTEGRATION_CONFIG);
            if (hiveIntegration) {
                final StorageSchemaCompatibility compatibility = StorageSchemaCompatibility.getCompatibility(connectorConfig.getString(Hdfs3SinkConnectorConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG));
                if (compatibility == StorageSchemaCompatibility.NONE) {
                    throw new ConfigException("Hive Integration requires schema compatibility to be BACKWARD, FORWARD or FULL");
                }
            }
            final Long interval = connectorConfig.getLong(Hdfs3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
            if (interval > 0L) {
                final String timeZoneString = connectorConfig.getString(PartitionerConfig.TIMEZONE_CONFIG);
                if (timeZoneString.equals("")) {
                    throw new ConfigException(timeZoneString, (Object)timeZoneString, "Timezone cannot be empty when using scheduled file rotation.");
                }
                DateTimeZone.forID(timeZoneString);
            }
            final int schemaCacheSize = connectorConfig.getInt(Hdfs3SinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG);
            this.avroData = new AvroData(schemaCacheSize);
            this.hdfsWriter = new DataWriter(connectorConfig, this.context, this.avroData);
            this.recover(assignment);
            if (hiveIntegration) {
                this.syncWithHive();
            }
        }
        catch (ConfigException e) {
            throw new ConnectException("Couldn't start Hdfs3SinkConnector due to configuration error.", (Throwable)e);
        }
        catch (ConnectException e2) {
            Hdfs3SinkTask.log.info("Couldn't start Hdfs3SinkConnector:", (Throwable)e2);
            Hdfs3SinkTask.log.info("Shutting down Hdfs3SinkConnector.");
            if (this.hdfsWriter != null) {
                this.hdfsWriter.close();
                this.hdfsWriter.stop();
            }
        }
        Hdfs3SinkTask.log.info("The connector relies on offsets in HDFS filenames, but does commit these offsets to Connect to enable monitoring progress of the HDFS connector. Upon startup, the HDFS Connector restores offsets from filenames in HDFS. In the absence of files in HDFS, the connector will attempt to find offsets for its consumer group in the '__consumer_offsets' topic. If offsets are not found, the consumer will rely on the reset policy specified in the 'consumer.auto.offset.reset' property to start exporting data to HDFS.");
    }
    
    public void put(final Collection<SinkRecord> records) throws ConnectException {
        if (Hdfs3SinkTask.log.isDebugEnabled()) {
            Hdfs3SinkTask.log.debug("Read {} records from Kafka", (Object)records.size());
        }
        try {
            this.hdfsWriter.write(records);
        }
        catch (ConnectException e) {
            throw new ConnectException((Throwable)e);
        }
    }
    
    public Map<TopicPartition, OffsetAndMetadata> preCommit(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        final Map<TopicPartition, OffsetAndMetadata> result = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (final Map.Entry<TopicPartition, Long> entry : this.hdfsWriter.getCommittedOffsets().entrySet()) {
            Hdfs3SinkTask.log.debug("Found last committed offset {} for topic partition {}", (Object)entry.getValue(), (Object)entry.getKey());
            result.put(entry.getKey(), new OffsetAndMetadata((long)entry.getValue()));
        }
        Hdfs3SinkTask.log.debug("Returning committed offsets {}", (Object)result);
        return result;
    }
    
    public void open(final Collection<TopicPartition> partitions) {
        this.hdfsWriter.open(partitions);
    }
    
    public void close(final Collection<TopicPartition> partitions) {
        if (this.hdfsWriter != null) {
            this.hdfsWriter.close();
        }
    }
    
    public void stop() throws ConnectException {
        if (this.hdfsWriter != null) {
            this.hdfsWriter.stop();
        }
    }
    
    private void recover(final Set<TopicPartition> assignment) {
        for (final TopicPartition tp : assignment) {
            this.hdfsWriter.recover(tp);
        }
    }
    
    private void syncWithHive() throws ConnectException {
        this.hdfsWriter.syncWithHive();
    }
    
    public AvroData getAvroData() {
        return this.avroData;
    }
    
    static {
        log = LoggerFactory.getLogger(Hdfs3SinkTask.class);
    }
}
