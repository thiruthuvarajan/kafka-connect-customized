// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.tools;

import org.apache.kafka.common.config.ConfigDef;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.common.utils.AppInfoParser;
import java.util.Map;
import org.apache.kafka.connect.source.SourceConnector;

public class SchemaSourceConnector extends SourceConnector
{
    private Map<String, String> config;
    
    public String version() {
        return AppInfoParser.getVersion();
    }
    
    public void start(final Map<String, String> props) {
        this.config = props;
    }
    
    public Class<? extends Task> taskClass() {
        return (Class<? extends Task>)SchemaSourceTask.class;
    }
    
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>();
        for (Integer i = 0; i < maxTasks; ++i) {
            final Map<String, String> props = new HashMap<String, String>(this.config);
            props.put("id", i.toString());
            configs.add(props);
        }
        return configs;
    }
    
    public void stop() {
    }
    
    public ConfigDef config() {
        return new ConfigDef();
    }
}
