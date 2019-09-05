// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.json;

import java.util.Iterator;
import java.util.Collection;
import org.apache.kafka.connect.data.Schema;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.format.SchemaFileReader;

public class JsonFileReader implements SchemaFileReader<Hdfs3SinkConnectorConfig, Path>
{
	@Override
    public Schema getSchema(final Hdfs3SinkConnectorConfig conf, final Path path) {
        return null;
    }
    
    public Collection<Object> readData(final Hdfs3SinkConnectorConfig conf, final Path path) {
        throw new UnsupportedOperationException();
    }
    
    public boolean hasNext() {
        throw new UnsupportedOperationException();
    }
    
    public Object next() {
        throw new UnsupportedOperationException();
    }
    
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }
    
    public void close() {
    }
}
