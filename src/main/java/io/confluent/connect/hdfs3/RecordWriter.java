// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3;

import java.io.IOException;

public interface RecordWriter<V>
{
    void write(final V p0) throws IOException;
    
    void close() throws IOException;
}
