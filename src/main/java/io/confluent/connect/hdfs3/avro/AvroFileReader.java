// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.avro;

import java.util.Iterator;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import java.io.IOException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.kafka.connect.data.Schema;
import io.confluent.connect.avro.AvroData;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.format.SchemaFileReader;

public class AvroFileReader implements SchemaFileReader<Hdfs3SinkConnectorConfig, Path>
{
    private AvroData avroData;
    
    public AvroFileReader(final AvroData avroData) {
        this.avroData = avroData;
    }
    
    public Schema getSchema(Hdfs3SinkConnectorConfig conf, Path path) {
        try {
            SeekableInput input = new FsInput(path, conf.getHadoopConfiguration());
            DatumReader<Object> reader = new GenericDatumReader<>();
             FileReader<Object> fileReader = DataFileReader.openReader(input, reader);
             org.apache.avro.Schema schema = fileReader.getSchema();
            fileReader.close();
            return this.avroData.toConnectSchema(schema);
        }
        catch (IOException e) {
            throw new DataException(e);
        }
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
