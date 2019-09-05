// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3;

import org.slf4j.LoggerFactory;
import org.apache.kafka.common.config.ConfigDef;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import io.confluent.connect.utils.Version;
import java.util.Map;
import org.slf4j.Logger;
import org.apache.kafka.connect.sink.SinkConnector;

public class Hdfs3SinkConnector extends SinkConnector
{
    private static final Logger log;
    private Map<String, String> configProperties;
    private Hdfs3SinkConnectorConfig config;
    
    @Override
    public String version() {
        return Version.forClass(this.getClass());
    }
    
    @Override
    public void start(final Map<String, String> props) throws ConnectException {
        try {
            this.configProperties = props;
            this.config = new Hdfs3SinkConnectorConfig(props);
        }
        catch (ConfigException e) {
            throw new ConnectException("Couldn't start Hdfs3SinkConnector due to configuration error", (Throwable)e);
        }
    }
    
    @Override
    public Class<? extends Task> taskClass() {
        return (Class<? extends Task>)Hdfs3SinkTask.class;
    }
    
    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final List<Map<String, String>> taskConfigs = new ArrayList<Map<String, String>>();
        final Map<String, String> taskProps = new HashMap<String, String>();
        taskProps.putAll(this.configProperties);
        for (int i = 0; i < maxTasks; ++i) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }
    
    @Override
    public void stop() throws ConnectException {
    }
    
    @Override
    public ConfigDef config() {
        return Hdfs3SinkConnectorConfig.getConfig();
    }
    
    static {
        log = LoggerFactory.getLogger(Hdfs3SinkConnector.class);
    }
}
