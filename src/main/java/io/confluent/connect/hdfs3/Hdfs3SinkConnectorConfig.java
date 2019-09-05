// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3;

import java.util.LinkedList;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.hdfs3.json.JsonFormat;
import java.util.Collection;
import org.apache.kafka.common.config.ConfigException;
import java.util.concurrent.ConcurrentMap;
import io.confluent.connect.hdfs3.avro.AvroFormat;
import io.confluent.connect.hdfs3.storage.HdfsStorage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.config.AbstractConfig;
import java.util.Set;
import io.confluent.connect.storage.common.ComposableConfig;
import java.util.Map;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.common.ParentValueRecommender;
import io.confluent.connect.storage.common.GenericRecommender;
import org.apache.kafka.common.config.ConfigDef;
import io.confluent.connect.storage.StorageSinkConnectorConfig;


public class Hdfs3SinkConnectorConfig extends StorageSinkConnectorConfig
{
    public static final String HDFS_URL_CONFIG = "hdfs.url";
    public static final String HDFS_URL_DOC = "The HDFS connection URL. This configuration has the format of hdfs://hostname:port and specifies the HDFS to export data to. This property is deprecated and will be removed in future releases. Use ``store.url`` instead.";
    public static final String HDFS_URL_DEFAULT;
    public static final String HDFS_URL_DISPLAY = "HDFS URL";
    public static final String HADOOP_CONF_DIR_CONFIG = "hadoop.conf.dir";
    public static final String HADOOP_CONF_DIR_DEFAULT = "";
    private static final String HADOOP_CONF_DIR_DOC = "The Hadoop configuration directory.";
    private static final String HADOOP_CONF_DIR_DISPLAY = "Hadoop Configuration Directory";
    public static final String HADOOP_HOME_CONFIG = "hadoop.home";
    public static final String HADOOP_HOME_DEFAULT = "";
    private static final String HADOOP_HOME_DOC = "The Hadoop home directory.";
    private static final String HADOOP_HOME_DISPLAY = "Hadoop home directory";
    public static final String LOGS_DIR_CONFIG = "logs.dir";
    public static final String LOGS_DIR_DOC = "Top level directory to store the write ahead logs.";
    public static final String LOGS_DIR_DEFAULT = "logs";
    public static final String LOGS_DIR_DISPLAY = "Logs directory";
    public static final String HDFS_AUTHENTICATION_KERBEROS_CONFIG = "hdfs.authentication.kerberos";
    private static final String HDFS_AUTHENTICATION_KERBEROS_DOC = "Configuration indicating whether HDFS is using Kerberos for authentication.";
    private static final boolean HDFS_AUTHENTICATION_KERBEROS_DEFAULT = false;
    private static final String HDFS_AUTHENTICATION_KERBEROS_DISPLAY = "HDFS Authentication Kerberos";
    public static final String CONNECT_HDFS_PRINCIPAL_CONFIG = "connect.hdfs.principal";
    public static final String CONNECT_HDFS_PRINCIPAL_DEFAULT = "";
    private static final String CONNECT_HDFS_PRINCIPAL_DOC = "The principal to use when HDFS is using Kerberos to for authentication.";
    private static final String CONNECT_HDFS_PRINCIPAL_DISPLAY = "Connect Kerberos Principal";
    public static final String CONNECT_HDFS_KEYTAB_CONFIG = "connect.hdfs.keytab";
    public static final String CONNECT_HDFS_KEYTAB_DEFAULT = "";
    private static final String CONNECT_HDFS_KEYTAB_DOC = "The path to the keytab file for the HDFS connector principal. This keytab file should only be readable by the connector user.";
    private static final String CONNECT_HDFS_KEYTAB_DISPLAY = "Connect Kerberos Keytab";
    public static final String HDFS_NAMENODE_PRINCIPAL_CONFIG = "hdfs.namenode.principal";
    public static final String HDFS_NAMENODE_PRINCIPAL_DEFAULT = "";
    private static final String HDFS_NAMENODE_PRINCIPAL_DOC = "The principal for HDFS Namenode.";
    private static final String HDFS_NAMENODE_PRINCIPAL_DISPLAY = "HDFS NameNode Kerberos Principal";
    public static final String KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG = "kerberos.ticket.renew.period.ms";
    public static final long KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT = 3600000L;
    private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DOC = "The period in milliseconds to renew the Kerberos ticket.";
    private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY = "Kerberos Ticket Renew Period (ms)";
    private static final ConfigDef.Recommender hdfsAuthenticationKerberosDependentsRecommender;
    private static final GenericRecommender STORAGE_CLASS_RECOMMENDER;
    private static final GenericRecommender FORMAT_CLASS_RECOMMENDER;
    private static final GenericRecommender PARTITIONER_CLASS_RECOMMENDER;
    private static final ParentValueRecommender AVRO_COMPRESSION_RECOMMENDER;
    private final String name;
    private final StorageCommonConfig commonConfig;
    private final HiveConfig hiveConfig;
    private final PartitionerConfig partitionerConfig;
    private final Map<String, ComposableConfig> propertyToConfig;
    private final Set<AbstractConfig> allConfigs;
    private Configuration hadoopConfig;
    
    public static ConfigDef newConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        String group = "HDFS";
        int orderInGroup = 0;
        configDef.define("hdfs.url", ConfigDef.Type.STRING, Hdfs3SinkConnectorConfig.HDFS_URL_DEFAULT, ConfigDef.Importance.HIGH, HDFS_URL_DOC, group, ++orderInGroup, ConfigDef.Width.MEDIUM, HDFS_URL_DISPLAY);
        configDef.define("hadoop.conf.dir", ConfigDef.Type.STRING, HADOOP_CONF_DIR_DEFAULT, ConfigDef.Importance.HIGH, HADOOP_CONF_DIR_DOC, group, ++orderInGroup, ConfigDef.Width.MEDIUM, HADOOP_CONF_DIR_DISPLAY);
        configDef.define("hadoop.home", ConfigDef.Type.STRING,HADOOP_HOME_DEFAULT, ConfigDef.Importance.HIGH, HADOOP_HOME_DOC,group, ++orderInGroup, ConfigDef.Width.SHORT, HADOOP_HOME_DISPLAY);
        configDef.define(LOGS_DIR_CONFIG, ConfigDef.Type.STRING, LOGS_DIR_DEFAULT, ConfigDef.Importance.HIGH, LOGS_DIR_DOC, group, ++orderInGroup, ConfigDef.Width.SHORT, LOGS_DIR_DISPLAY);
        group = "Security";
        orderInGroup = 0;
        configDef.define(HDFS_AUTHENTICATION_KERBEROS_CONFIG, ConfigDef.Type.BOOLEAN, HDFS_AUTHENTICATION_KERBEROS_DEFAULT, ConfigDef.Importance.HIGH, HDFS_AUTHENTICATION_KERBEROS_DOC, group, ++orderInGroup, ConfigDef.Width.SHORT, HDFS_AUTHENTICATION_KERBEROS_DISPLAY, Arrays.asList("connect.hdfs.principal", "connect.hdfs.keytab", "hdfs.namenode.principal", "kerberos.ticket.renew.period.ms"));
        configDef.define(CONNECT_HDFS_PRINCIPAL_CONFIG, ConfigDef.Type.STRING, CONNECT_HDFS_PRINCIPAL_DEFAULT, ConfigDef.Importance.HIGH,CONNECT_HDFS_PRINCIPAL_DOC, group, ++orderInGroup, ConfigDef.Width.MEDIUM, CONNECT_HDFS_PRINCIPAL_DISPLAY, Hdfs3SinkConnectorConfig.hdfsAuthenticationKerberosDependentsRecommender);
        configDef.define(CONNECT_HDFS_KEYTAB_CONFIG, ConfigDef.Type.STRING, CONNECT_HDFS_KEYTAB_DEFAULT, ConfigDef.Importance.HIGH, CONNECT_HDFS_KEYTAB_DOC, group, ++orderInGroup, ConfigDef.Width.MEDIUM, CONNECT_HDFS_KEYTAB_DISPLAY, Hdfs3SinkConnectorConfig.hdfsAuthenticationKerberosDependentsRecommender);
        configDef.define(HDFS_NAMENODE_PRINCIPAL_CONFIG, ConfigDef.Type.STRING, HDFS_NAMENODE_PRINCIPAL_DEFAULT, ConfigDef.Importance.HIGH, HDFS_NAMENODE_PRINCIPAL_DOC, group, ++orderInGroup, ConfigDef.Width.MEDIUM, HDFS_NAMENODE_PRINCIPAL_DISPLAY, Hdfs3SinkConnectorConfig.hdfsAuthenticationKerberosDependentsRecommender);
        configDef.define(KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG, ConfigDef.Type.LONG, KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT, ConfigDef.Importance.LOW,KERBEROS_TICKET_RENEW_PERIOD_MS_DOC, group, ++orderInGroup, ConfigDef.Width.SHORT, KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY, Hdfs3SinkConnectorConfig.hdfsAuthenticationKerberosDependentsRecommender);
        final ConfigDef storageConfigDef = StorageSinkConnectorConfig.newConfigDef((ConfigDef.Recommender)Hdfs3SinkConnectorConfig.FORMAT_CLASS_RECOMMENDER, (ConfigDef.Recommender)Hdfs3SinkConnectorConfig.AVRO_COMPRESSION_RECOMMENDER);
        for (final ConfigDef.ConfigKey key : storageConfigDef.configKeys().values()) {
            configDef.define(key);
        }
        return configDef;
    }
    
    public Hdfs3SinkConnectorConfig(final Map<String, String> props) {
        this(newConfigDef(), addDefaults(props));
    }
    
    protected Hdfs3SinkConnectorConfig(final ConfigDef configDef, final Map<String, String> props) {
        super(configDef, props);
        this.propertyToConfig = new HashMap<String, ComposableConfig>();
        this.allConfigs = new HashSet<AbstractConfig>();
        final ConfigDef storageCommonConfigDef = StorageCommonConfig.newConfigDef((ConfigDef.Recommender)Hdfs3SinkConnectorConfig.STORAGE_CLASS_RECOMMENDER);
        this.commonConfig = new StorageCommonConfig(storageCommonConfigDef, this.originalsStrings());
        this.hiveConfig = new HiveConfig(this.originalsStrings());
        final ConfigDef partitionerConfigDef = PartitionerConfig.newConfigDef((ConfigDef.Recommender)Hdfs3SinkConnectorConfig.PARTITIONER_CLASS_RECOMMENDER);
        this.partitionerConfig = new PartitionerConfig(partitionerConfigDef, this.originalsStrings());
        this.name = parseName(this.originalsStrings());
        this.hadoopConfig = new Configuration();
        this.addToGlobal((AbstractConfig)this.hiveConfig);
        this.addToGlobal((AbstractConfig)this.partitionerConfig);
        this.addToGlobal((AbstractConfig)this.commonConfig);
        this.addToGlobal((AbstractConfig)this);
    }
    
    public static Map<String, String> addDefaults(final Map<String, String> props) {
        final ConcurrentMap<String, String> propsCopy = new ConcurrentHashMap<String, String>(props);
        propsCopy.putIfAbsent(io.confluent.connect.storage.common.StorageCommonConfig.STORAGE_CLASS_CONFIG, HdfsStorage.class.getName());
        propsCopy.putIfAbsent(Hdfs3SinkConnectorConfig.FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
        return propsCopy;
    }
    
    protected static String parseName(final Map<String, String> props) {
        final String nameProp = props.get("name");
        return (nameProp != null) ? nameProp : "HDFS-sink";
    }
    
    private void addToGlobal(final AbstractConfig config) {
        this.allConfigs.add(config);
        this.addConfig(config.values(), (ComposableConfig)config);
    }
    
    private void addConfig(final Map<String, ?> parsedProps, final ComposableConfig config) {
        for (final String key : parsedProps.keySet()) {
            this.propertyToConfig.put(key, config);
        }
    }
    
    public String getName() {
        return this.name;
    }
    
    public Object get(final String key) {
        final ComposableConfig config = this.propertyToConfig.get(key);
        if (config == null) {
            throw new ConfigException(String.format("Unknown configuration '%s'", key));
        }
        return (config == this) ? super.get(key) : config.get(key);
    }
    
    public Configuration getHadoopConfiguration() {
        return this.hadoopConfig;
    }
    
    public Map<String, ?> plainValues() {
        final Map<String, Object> map = new HashMap<String, Object>();
        for (final AbstractConfig config : this.allConfigs) {
            map.putAll(config.values());
        }
        final Map<String, ?> originals = (Map<String, ?>)this.originals();
        for (final String originalKey : originals.keySet()) {
            if (!map.containsKey(originalKey)) {
                map.put(originalKey, originals.get(originalKey));
            }
        }
        return map;
    }
    
    public static ConfigDef getConfig() {
        final Set<String> skip = new HashSet<String>();
        skip.add(io.confluent.connect.storage.common.StorageCommonConfig.STORAGE_CLASS_CONFIG);
        skip.add(FORMAT_CLASS_CONFIG);
        final ConfigDef visible = new ConfigDef();
        addAllConfigKeys(visible, newConfigDef(), skip);
        addAllConfigKeys(visible, StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER), skip);
        addAllConfigKeys(visible, PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER), skip);
        addAllConfigKeys(visible, HiveConfig.getConfig(), skip);
        visible.define(io.confluent.connect.storage.common.StorageCommonConfig.STORAGE_CLASS_CONFIG, ConfigDef.Type.CLASS, HdfsStorage.class.getName(), ConfigDef.Importance.HIGH, io.confluent.connect.storage.common.StorageCommonConfig.STORAGE_CLASS_DOC, "Storage", 1, ConfigDef.Width.NONE, io.confluent.connect.storage.common.StorageCommonConfig.STORAGE_CLASS_DISPLAY, STORAGE_CLASS_RECOMMENDER);
        visible.define(FORMAT_CLASS_CONFIG, ConfigDef.Type.CLASS, AvroFormat.class.getName(), ConfigDef.Importance.HIGH, FORMAT_CLASS_DOC, "Connector", 1, ConfigDef.Width.NONE, FORMAT_CLASS_DISPLAY, FORMAT_CLASS_RECOMMENDER);
        return visible;
    }
    
    private static void addAllConfigKeys(final ConfigDef container, final ConfigDef other, final Set<String> skip) {
        for (final ConfigDef.ConfigKey key : other.configKeys().values()) {
            if (skip != null && !skip.contains(key.name) && !container.configKeys().containsKey(key.name)) {
                container.define(key);
            }
        }
    }
    
    public static void main(final String[] args) {
        System.out.println(getConfig().toEnrichedRst());
    }
    
    static {
        HDFS_URL_DEFAULT = null;
        hdfsAuthenticationKerberosDependentsRecommender = (ConfigDef.Recommender)new BooleanParentRecommender(HDFS_AUTHENTICATION_KERBEROS_CONFIG);
        STORAGE_CLASS_RECOMMENDER = new GenericRecommender();
        FORMAT_CLASS_RECOMMENDER = new GenericRecommender();
        PARTITIONER_CLASS_RECOMMENDER = new GenericRecommender();
        AVRO_COMPRESSION_RECOMMENDER = new ParentValueRecommender(FORMAT_CLASS_CONFIG, AvroFormat.class, AVRO_SUPPORTED_CODECS);
        Hdfs3SinkConnectorConfig.STORAGE_CLASS_RECOMMENDER.addValidValues(Arrays.<Object>asList(HdfsStorage.class));
        Hdfs3SinkConnectorConfig.FORMAT_CLASS_RECOMMENDER.addValidValues(Arrays.<Object>asList(AvroFormat.class, JsonFormat.class));
        Hdfs3SinkConnectorConfig.PARTITIONER_CLASS_RECOMMENDER.addValidValues(Arrays.<Object>asList(DefaultPartitioner.class, HourlyPartitioner.class, DailyPartitioner.class, TimeBasedPartitioner.class, FieldPartitioner.class));
    }
    
    private static class BooleanParentRecommender implements ConfigDef.Recommender
    {
        protected String parentConfigName;
        
        public BooleanParentRecommender(final String parentConfigName) {
            this.parentConfigName = parentConfigName;
        }
        
        public List<Object> validValues(final String name, final Map<String, Object> connectorConfigs) {
            return new LinkedList<>();
        }
        
        public boolean visible(final String name, final Map<String, Object> connectorConfigs) {
            return (Boolean) connectorConfigs.get(this.parentConfigName);
        }
    }
}
