// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.wal;

import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import org.apache.hadoop.io.Writable;

public class WALEntry implements Writable
{
    private String name;
    
    public WALEntry(final String name) {
        this.name = name;
    }
    
    public WALEntry() {
        this.name = null;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void readFields(final DataInput in) throws IOException {
        this.name = Text.readString(in);
    }
    
    public void write(final DataOutput out) throws IOException {
        Text.writeString(out, this.name);
    }
}
