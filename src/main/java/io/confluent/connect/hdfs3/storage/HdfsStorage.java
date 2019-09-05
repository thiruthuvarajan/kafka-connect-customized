// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.storage;

import org.apache.avro.mapred.FsInput;
import org.apache.avro.file.SeekableInput;
import io.confluent.connect.hdfs3.wal.FSWAL;
import io.confluent.connect.storage.wal.WAL;
import org.apache.kafka.common.TopicPartition;
import java.io.OutputStream;
import org.apache.kafka.connect.errors.ConnectException;
import java.util.Arrays;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import java.util.List;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import io.confluent.connect.storage.Storage;

public class HdfsStorage implements Storage<Hdfs3SinkConnectorConfig, List<FileStatus>>
{
    private final FileSystem fs;
    private final Hdfs3SinkConnectorConfig conf;
    private final String url;
    
    protected HdfsStorage(final Hdfs3SinkConnectorConfig conf, final String url, final FileSystem fs) {
        this.conf = conf;
        this.url = url;
        this.fs = fs;
    }
    
    public HdfsStorage(final Hdfs3SinkConnectorConfig conf, final String url) throws IOException {
        this.conf = conf;
        this.url = url;
        this.fs = FileSystem.newInstance(URI.create(url), conf.getHadoopConfiguration());
    }
    
    public List<FileStatus> list(final String path, final PathFilter filter) {
        try {
            return Arrays.asList(this.fs.listStatus(new Path(path), filter));
        }
        catch (IOException e) {
            throw new ConnectException((Throwable)e);
        }
    }
    
    public List<FileStatus> list(final String path) {
        try {
            return Arrays.asList(this.fs.listStatus(new Path(path)));
        }
        catch (IOException e) {
            throw new ConnectException((Throwable)e);
        }
    }
    
    public OutputStream append(final String filename) {
        throw new UnsupportedOperationException();
    }
    
    public boolean create(final String filename) {
        try {
            return this.fs.mkdirs(new Path(filename));
        }
        catch (IOException e) {
            throw new ConnectException((Throwable)e);
        }
    }
    
    public OutputStream create(final String filename, final boolean overwrite) {
        return this.create(filename, this.conf, overwrite);
    }
    
    public OutputStream create(final String filename, final Hdfs3SinkConnectorConfig conf, final boolean overwrite) {
        try {
            final Path path = new Path(filename);
            return (OutputStream)path.getFileSystem(conf.getHadoopConfiguration()).create(path);
        }
        catch (IOException e) {
            throw new ConnectException((Throwable)e);
        }
    }
    
    public boolean exists(final String filename) {
        try {
            return this.fs.exists(new Path(filename));
        }
        catch (IOException e) {
            throw new ConnectException((Throwable)e);
        }
    }
    
    public void commit(final String tempFile, final String committedFile) {
        this.renameFile(tempFile, committedFile);
    }
    
    public void delete(final String filename) {
        try {
            this.fs.delete(new Path(filename), true);
        }
        catch (IOException e) {
            throw new ConnectException((Throwable)e);
        }
    }
    
    public void close() {
        if (this.fs != null) {
            try {
                this.fs.close();
            }
            catch (IOException e) {
                throw new ConnectException((Throwable)e);
            }
        }
    }
    
    public WAL wal(final String topicsDir, final TopicPartition topicPart) {
        return (WAL)new FSWAL(topicsDir, topicPart, this);
    }
    
    public Hdfs3SinkConnectorConfig conf() {
        return this.conf;
    }
    
    public String url() {
        return this.url;
    }
    
    private void renameFile(final String sourcePath, final String targetPath) {
        if (sourcePath.equals(targetPath)) {
            return;
        }
        try {
            final Path srcPath = new Path(sourcePath);
            final Path dstPath = new Path(targetPath);
            if (this.fs.exists(srcPath)) {
                this.fs.rename(srcPath, dstPath);
            }
        }
        catch (IOException e) {
            throw new ConnectException((Throwable)e);
        }
    }
    
    public SeekableInput open(final String filename, final Hdfs3SinkConnectorConfig conf) {
        try {
            return (SeekableInput)new FsInput(new Path(filename), conf.getHadoopConfiguration());
        }
        catch (IOException e) {
            throw new ConnectException((Throwable)e);
        }
    }
}
