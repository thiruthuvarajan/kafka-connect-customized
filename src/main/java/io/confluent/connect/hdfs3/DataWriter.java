// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3;

import io.confluent.common.utils.SystemTime;
import org.slf4j.LoggerFactory;
import io.confluent.connect.storage.format.RecordWriter;
import java.util.concurrent.TimeUnit;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import io.confluent.connect.hdfs3.filter.CommittedFileFilter;
import io.confluent.connect.hdfs3.filter.TopicCommittedFileFilter;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.hadoop.conf.Configuration;
import java.lang.reflect.InvocationTargetException;
import io.confluent.connect.storage.Storage;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.common.config.AbstractConfig;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.kafka.connect.data.Schema;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.kafka.common.config.ConfigException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import java.util.concurrent.Future;
import java.util.Queue;
import io.confluent.connect.storage.hive.HiveUtil;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.hive.HiveMetaStore;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.connect.sink.SinkTaskContext;
import io.confluent.connect.avro.AvroData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

import java.util.Set;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.hdfs3.storage.HdfsStorage;
import org.apache.kafka.common.TopicPartition;
import java.util.Map;
import io.confluent.common.utils.Time;
import org.slf4j.Logger;

public class DataWriter
{
    private static final Logger log;
    private static final Time SYSTEM_TIME;
    private final Time time;
    private Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
    private String url;
    private HdfsStorage storage;
    private String topicsDir;
    private Format format;
    private RecordWriterProvider writerProvider;
    private io.confluent.connect.storage.format.RecordWriterProvider<Hdfs3SinkConnectorConfig> newWriterProvider;
    private SchemaFileReader<Hdfs3SinkConnectorConfig, Path> schemaFileReader;
    private io.confluent.connect.storage.format.Format<Hdfs3SinkConnectorConfig, Path> newFormat;
    private Set<TopicPartition> assignment;
    private Partitioner<FieldSchema> partitioner;
    private Hdfs3SinkConnectorConfig connectorConfig;
    private AvroData avroData;
    private SinkTaskContext context;
    private ExecutorService executorService;
    private String hiveDatabase;
    private HiveMetaStore hiveMetaStore;
    private HiveUtil hive;
    private Queue<Future<Void>> hiveUpdateFutures;
    private boolean hiveIntegration;
    private Thread ticketRenewThread;
    private volatile boolean isRunning;
    
    public DataWriter(Hdfs3SinkConnectorConfig connectorConfig, SinkTaskContext context, AvroData avroData) {
        this(connectorConfig, context, avroData, DataWriter.SYSTEM_TIME);
    }
    
    @SuppressWarnings("unchecked")
    public DataWriter(Hdfs3SinkConnectorConfig connectorConfig, SinkTaskContext context, AvroData avroData, Time time) {
        this.time = time;
        try {
            String hadoopHome = connectorConfig.getString(Hdfs3SinkConnectorConfig.HADOOP_HOME_CONFIG);
            System.setProperty("hadoop.home.dir", hadoopHome);
            this.connectorConfig = connectorConfig;
            this.avroData = avroData;
            this.context = context;
            
            String hadoopConfDir = connectorConfig.getString(Hdfs3SinkConnectorConfig.HADOOP_CONF_DIR_CONFIG);
            
            DataWriter.log.info("Hadoop configuration directory {}", (Object)hadoopConfDir);
            final Configuration conf = connectorConfig.getHadoopConfiguration();
            if (!hadoopConfDir.equals("")) {
                conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
                conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));
            }
            
            boolean secureHadoop = connectorConfig.getBoolean(Hdfs3SinkConnectorConfig.HDFS_AUTHENTICATION_KERBEROS_CONFIG);
            
            if (secureHadoop) {
                SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
                String principalConfig = connectorConfig.getString(Hdfs3SinkConnectorConfig.CONNECT_HDFS_PRINCIPAL_CONFIG);
                final String keytab = connectorConfig.getString(Hdfs3SinkConnectorConfig.CONNECT_HDFS_KEYTAB_CONFIG);
             
                if (principalConfig == null || keytab == null) {
                    throw new ConfigException("Hadoop is using Kerberos for authentication, you need to provide both a connect principal and the path to the keytab of the principal.");
                }
                
                conf.set("hadoop.security.authentication", "kerberos");
                conf.set("hadoop.security.authorization", "true");
                String hostname = InetAddress.getLocalHost().getCanonicalHostName();
                String namenodePrincipalConfig = connectorConfig.getString(Hdfs3SinkConnectorConfig.HDFS_NAMENODE_PRINCIPAL_CONFIG);
                String namenodePrincipal = SecurityUtil.getServerPrincipal(namenodePrincipalConfig, hostname);
                
                if (conf.get("dfs.namenode.kerberos.principal") == null) {
                    conf.set("dfs.namenode.kerberos.principal", namenodePrincipal);
                }
                
                DataWriter.log.info("Hadoop namenode principal: " + conf.get("dfs.namenode.kerberos.principal"));
                UserGroupInformation.setConfiguration(conf);
                final String principal = SecurityUtil.getServerPrincipal(principalConfig, hostname);
                UserGroupInformation.loginUserFromKeytab(principal, keytab);
                final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
                DataWriter.log.info("Login as: " + ugi.getUserName());
                final long renewPeriod = connectorConfig.getLong(Hdfs3SinkConnectorConfig.KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG);
              
                this.isRunning = true;
                this.ticketRenewThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (DataWriter.this) {
                            while (DataWriter.this.isRunning) {
                                try {
                                    DataWriter.this.wait(renewPeriod);
                                    if (!DataWriter.this.isRunning) {
                                        continue;
                                    }
                                    ugi.reloginFromKeytab();
                                }
                                catch (IOException e) {
                                    DataWriter.log.error("Error renewing the ticket", e);
                                }
                                catch (InterruptedException ex) {}
                            }
                        }
                    }
                });
                DataWriter.log.info("Starting the Kerberos ticket renew thread with period {}ms.", renewPeriod);
                this.ticketRenewThread.start();
            }
            
            url = connectorConfig.getString(Hdfs3SinkConnectorConfig.HDFS_URL_CONFIG);
            topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
            
            @SuppressWarnings("unchecked")
			Class<? extends HdfsStorage> storageClass = (Class<? extends HdfsStorage>)connectorConfig
										.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
            
            storage = StorageFactory.createStorage(storageClass, 
            		Hdfs3SinkConnectorConfig.class, 
            		connectorConfig, 
            		url);
            createDir(topicsDir);
            createDir(topicsDir + "/+tmp/");
            String logsDir = connectorConfig.getString(Hdfs3SinkConnectorConfig.LOGS_DIR_CONFIG);
            createDir(logsDir);
            try {
				Class<io.confluent.connect.storage.format.Format> formatClass = (Class<io.confluent.connect.storage.format.Format>)
                																connectorConfig.getClass(Hdfs3SinkConnectorConfig.FORMAT_CLASS_CONFIG);
                newFormat = formatClass.getConstructor(HdfsStorage.class).newInstance(storage);
                newWriterProvider = newFormat.getRecordWriterProvider();
                schemaFileReader = newFormat.getSchemaFileReader();
            }
            catch (NoSuchMethodException e3) {
                Class<Format> formatClass2 = (Class<Format>)
                		connectorConfig.getClass(Hdfs3SinkConnectorConfig.FORMAT_CLASS_CONFIG);
                format = formatClass2.getConstructor().newInstance();
                writerProvider = format.getRecordWriterProvider();
                final SchemaFileReader<Hdfs3SinkConnectorConfig, Path> oldReader = format.getSchemaFileReader(avroData);
                schemaFileReader = (SchemaFileReader<Hdfs3SinkConnectorConfig, Path>)new SchemaFileReader<Hdfs3SinkConnectorConfig, Path>() {
                	@Override
                    public Schema getSchema(final Hdfs3SinkConnectorConfig hdfs3SinkConnectorConfig, final Path path) {
                        return oldReader.getSchema(hdfs3SinkConnectorConfig, path);
                    }
                    
                	@Override
                    public Iterator<Object> iterator() {
                        throw new UnsupportedOperationException();
                    }
                    
                	@Override
                    public boolean hasNext() {
                        throw new UnsupportedOperationException();
                    }
                    
                	@Override
                    public Object next() {
                        throw new UnsupportedOperationException();
                    }
                    
                	@Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                    
                	@Override
                    public void close() {
                    }
                };
            }
            

            partitioner = newPartitioner(connectorConfig);
            assignment = new HashSet<TopicPartition>(context.assignment());
            hiveIntegration = connectorConfig.getBoolean(HiveConfig.HIVE_INTEGRATION_CONFIG);
            if (this.hiveIntegration) {
                this.hiveDatabase = connectorConfig.getString(HiveConfig.HIVE_DATABASE_CONFIG);
                this.hiveMetaStore = new HiveMetaStore(conf, (AbstractConfig)connectorConfig);
                if (format != null) {
                    this.hive = this.format.getHiveUtil(connectorConfig, this.hiveMetaStore);
                }
                else {
                    if (this.newFormat == null) {
                        throw new ConnectException("One of old or new format classes must be provided");
                    }
                    final HiveUtil newHiveUtil = this.newFormat.getHiveFactory().createHiveUtil((AbstractConfig)connectorConfig, this.hiveMetaStore);
                    this.hive = new HiveUtil(connectorConfig, this.hiveMetaStore) {
                    	@Override
                        public void createTable(String database, String tableName, Schema schema, Partitioner<FieldSchema> partitioner) {
                            newHiveUtil.createTable(database, tableName, schema, partitioner);
                        }
                        
                    	@Override
                        public void alterSchema(String database,  String tableName,  Schema schema) {
                            newHiveUtil.alterSchema(database, tableName, schema);
                        }
                    };
                }
                this.executorService = Executors.newSingleThreadExecutor();
                this.hiveUpdateFutures = new LinkedList<Future<Void>>();
            }
            this.topicPartitionWriters = new HashMap<TopicPartition, TopicPartitionWriter>();
            for (final TopicPartition tp : this.assignment) {
                final TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(tp, (Storage<Hdfs3SinkConnectorConfig, ?>)this.storage, this.writerProvider, this.newWriterProvider, this.partitioner, connectorConfig, context, avroData, this.hiveMetaStore, this.hive, this.schemaFileReader, this.executorService, this.hiveUpdateFutures, time);
                this.topicPartitionWriters.put(tp, topicPartitionWriter);
            }
        }
        catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException ex2) {
         
            final ReflectiveOperationException e = ex2;
            throw new ConnectException("Reflection exception: ", (Throwable)e);
        }
        catch (IOException e2) {
            throw new ConnectException(e2);
        }
    }
    
    public void write(Collection<SinkRecord> records) {
        for ( SinkRecord record : records) {
             String topic = record.topic();
             int partition = record.kafkaPartition();
             TopicPartition tp = new TopicPartition(topic, partition);
             topicPartitionWriters.get(tp).buffer(record);
        }
        if (this.hiveIntegration) {
            final Iterator<Future<Void>> iterator = this.hiveUpdateFutures.iterator();
            while (iterator.hasNext()) {
                try {
                    final Future<Void> future = iterator.next();
                    if (future.isDone()) {
                        future.get();
                        iterator.remove();
                        continue;
                    }
                }
                catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
                catch (InterruptedException ex) {
                    continue;
                }
                break;
            }
        }
        for (final TopicPartition tp2 : this.assignment) {
            this.topicPartitionWriters.get(tp2).write();
        }
    }
    
    public void recover(final TopicPartition tp) {
        this.topicPartitionWriters.get(tp).recover();
    }
    
    public void syncWithHive() throws ConnectException {
        final Set<String> topics = new HashSet<String>();
        for (final TopicPartition tp : this.assignment) {
            topics.add(tp.topic());
        }
        try {
            for (final String topic : topics) {
                final String topicDir = FileUtils.topicDirectory(this.url, this.topicsDir, topic);
                final CommittedFileFilter filter = new TopicCommittedFileFilter(topic);
                final FileStatus fileStatusWithMaxOffset = FileUtils.fileStatusWithMaxOffset(this.storage, new Path(topicDir), filter);
                if (fileStatusWithMaxOffset != null) {
                    Path path = fileStatusWithMaxOffset.getPath();
                    Schema latestSchema = this.schemaFileReader.getSchema(this.connectorConfig, path);
                    hive.createTable(this.hiveDatabase, topic, latestSchema, partitioner);
                    List<String> partitions = hiveMetaStore.listPartitions(this.hiveDatabase, topic, (short)(-1));
                    FileStatus[] directories;
                    FileStatus[] statuses = directories = FileUtils.getDirectories(this.storage, new Path(topicDir));
                    for (FileStatus status : directories) {
                         String location = status.getPath().toString();
                        if (!partitions.contains(location)) {
                             String partitionValue = this.getPartitionValue(location);
                            this.hiveMetaStore.addPartition(this.hiveDatabase, topic, partitionValue);
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new ConnectException((Throwable)e);
        }
    }
    
    public void open(final Collection<TopicPartition> partitions) {
        this.assignment = new HashSet<TopicPartition>(partitions);
        for (final TopicPartition tp : this.assignment) {
            final TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(tp, (Storage<Hdfs3SinkConnectorConfig, ?>)this.storage, this.writerProvider, this.newWriterProvider, this.partitioner, this.connectorConfig, this.context, this.avroData, this.hiveMetaStore, this.hive, this.schemaFileReader, this.executorService, this.hiveUpdateFutures, this.time);
            this.topicPartitionWriters.put(tp, topicPartitionWriter);
            this.recover(tp);
        }
    }
    
    public void close() {
        for (final TopicPartition tp : this.assignment) {
            try {
                this.topicPartitionWriters.get(tp).close();
            }
            catch (ConnectException e) {
                DataWriter.log.error("Error closing writer for {}. Error: {}", (Object)tp, (Object)e.getMessage());
            }
        }
        this.topicPartitionWriters.clear();
        this.assignment.clear();
    }
    
    public void stop() {
        if (this.executorService != null) {
            boolean terminated = false;
            try {
                DataWriter.log.info("Shutting down Hive executor service.");
                this.executorService.shutdown();
                final long shutDownTimeout = this.connectorConfig.getLong("shutdown.timeout.ms");
                DataWriter.log.info("Awaiting termination.");
                terminated = this.executorService.awaitTermination(shutDownTimeout, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ex) {}
            if (!terminated) {
                DataWriter.log.warn("Unclean Hive executor service shutdown, you probably need to sync with Hive next time you start the connector");
                this.executorService.shutdownNow();
            }
        }
        this.storage.close();
        if (this.ticketRenewThread != null) {
            synchronized (this) {
                this.isRunning = false;
                this.notifyAll();
            }
        }
    }
    
    public Partitioner<FieldSchema> getPartitioner() {
        return this.partitioner;
    }
    
    public Map<TopicPartition, Long> getCommittedOffsets() {
        final Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>();
        DataWriter.log.debug("Writer looking for last offsets for topic partitions {}", (Object)this.assignment);
        for (final TopicPartition tp : this.assignment) {
            final long committedOffset = this.topicPartitionWriters.get(tp).offset();
            DataWriter.log.debug("Writer found last offset {} for topic partition {}", (Object)committedOffset, (Object)tp);
            if (committedOffset >= 0L) {
                offsets.put(tp, committedOffset);
            }
        }
        return offsets;
    }
    
    public TopicPartitionWriter getBucketWriter(final TopicPartition tp) {
        return this.topicPartitionWriters.get(tp);
    }
    
    public Storage<Hdfs3SinkConnectorConfig, ?> getStorage() {
        return (Storage<Hdfs3SinkConnectorConfig, ?>)this.storage;
    }
    
    Map<String, RecordWriter> getWriters(final TopicPartition tp) {
        return this.topicPartitionWriters.get(tp).getWriters();
    }
    
    public Map<String, String> getTempFileNames(final TopicPartition tp) {
        final TopicPartitionWriter topicPartitionWriter = this.topicPartitionWriters.get(tp);
        return topicPartitionWriter.getTempFiles();
    }
    
    private void createDir(final String dir) {
        final String path = this.url + "/" + dir;
        if (!this.storage.exists(path)) {
            this.storage.create(path);
        }
    }
    
    private Partitioner<FieldSchema> newPartitioner(final Hdfs3SinkConnectorConfig config) throws IllegalAccessException, InstantiationException {
        Partitioner<FieldSchema> partitioner;
        try {
            @SuppressWarnings("unchecked")
			Class<? extends Partitioner<FieldSchema>> partitionerClass = (Class<? extends Partitioner<FieldSchema>>)config.getClass(PartitionerConfig.PARTITIONER_CLASS_CONFIG);
            partitioner = partitionerClass.newInstance();
        }
        catch (ClassCastException e) {
            @SuppressWarnings("unchecked")
			Class<? extends Partitioner<FieldSchema>> partitionerClass2 = (Class<? extends Partitioner<FieldSchema>>)config.getClass(PartitionerConfig.PARTITIONER_CLASS_CONFIG);
            partitioner = new PartitionerWrapper((Partitioner<FieldSchema>)partitionerClass2.newInstance());
        }
        partitioner.configure(new HashMap<>(config.plainValues()));
        return partitioner;
    }
    
    private String getPartitionValue(final String path) {
        final String[] parts = path.split("/");
        final StringBuilder sb = new StringBuilder();
        sb.append("/");
        for (int i = 3; i < parts.length; ++i) {
            sb.append(parts[i]);
            sb.append("/");
        }
        return sb.toString();
    }
    
    static {
        log = LoggerFactory.getLogger((Class)DataWriter.class);
        SYSTEM_TIME = (Time)new SystemTime();
    }
    
    public static class PartitionerWrapper implements Partitioner<FieldSchema>
    {
        public final Partitioner<FieldSchema> partitioner;
        
        
        public PartitionerWrapper(Partitioner<FieldSchema> partitioner) {
            this.partitioner = partitioner;
        }
        
        @Override
        public void configure(final Map<String, Object> config) {
            this.partitioner.configure(config);
        }
        
        @Override
        public String encodePartition(final SinkRecord sinkRecord) {
            return partitioner.encodePartition(sinkRecord);
        }
        
        @Override
        public String encodePartition(final SinkRecord sinkRecord, final long nowInMillis) {
            return partitioner.encodePartition(sinkRecord, nowInMillis);
        }
        
        @Override
        public String generatePartitionedPath(final String topic, final String encodedPartition) {
            return this.partitioner.generatePartitionedPath(topic, encodedPartition);
        }
        
        @Override
        public List<FieldSchema> partitionFields() {
            return partitioner.partitionFields();
        }
    }
}
