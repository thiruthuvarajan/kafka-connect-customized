// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.parquet;

import java.util.Iterator;
import java.io.IOException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.kafka.connect.data.Schema;
import io.confluent.connect.avro.AvroData;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.format.SchemaFileReader;

public class ParquetFileReader implements SchemaFileReader<Hdfs3SinkConnectorConfig, Path>
{
    private AvroData avroData;
    
    public ParquetFileReader(final AvroData avroData) {
        this.avroData = avroData;
    }
    
    public Schema getSchema(final Hdfs3SinkConnectorConfig conf, final Path path) {
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
        ParquetReader.Builder<GenericRecord> builder = ParquetReader.builder(readSupport, path);
        try {
            ParquetReader<GenericRecord> parquetReader = builder.withConf(conf.getHadoopConfiguration()).build();
            Schema schema = null;
            GenericRecord record;
            while ((record = parquetReader.read()) != null) {
                schema = this.avroData.toConnectSchema(record.getSchema());
            }
            parquetReader.close();
            return schema;
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
