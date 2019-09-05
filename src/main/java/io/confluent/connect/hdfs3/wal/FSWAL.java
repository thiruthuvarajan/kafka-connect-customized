// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.wal;

import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Writable;
import java.util.HashMap;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.ConnectException;
import io.confluent.connect.hdfs3.FileUtils;
import org.apache.kafka.common.TopicPartition;
import io.confluent.connect.hdfs3.storage.HdfsStorage;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import org.slf4j.Logger;
import io.confluent.connect.storage.wal.WAL;

public class FSWAL implements WAL
{
    private static final Logger log;
    private WALFile.Writer writer;
    private WALFile.Reader reader;
    private String logFile;
    private Hdfs3SinkConnectorConfig conf;
    private HdfsStorage storage;
    
    public FSWAL(final String logsDir, final TopicPartition topicPart, final HdfsStorage storage) throws ConnectException {
        this.writer = null;
        this.reader = null;
        this.logFile = null;
        this.conf = null;
        this.storage = null;
        this.storage = storage;
        this.conf = storage.conf();
        final String url = storage.url();
        this.logFile = FileUtils.logFileName(url, logsDir, topicPart);
    }
    
    public void append(final String tempFile, final String committedFile) throws ConnectException {
        try {
            this.acquireLease();
            WALEntry key = new WALEntry(tempFile);
            WALEntry value = new WALEntry(committedFile);
            this.writer.append(key, value);
            this.writer.hsync();
        }
        catch (IOException e) {
            FSWAL.log.error("Error appending WAL file: {}, {}", (Object)this.logFile, (Object)e);
            this.close();
            throw new DataException((Throwable)e);
        }
    }
    
    public void acquireLease() throws ConnectException {
        long sleepIntervalMs = 1000L;
        while (sleepIntervalMs < 16000L) {
            try {
                if (this.writer == null) {
                    this.writer = WALFile.createWriter(this.conf, WALFile.Writer.file(new Path(this.logFile)), WALFile.Writer.appendIfExists(true));
                    FSWAL.log.info("Successfully acquired lease for {}", (Object)this.logFile);
                }
            }
            catch (RemoteException e) {
                if (e.getClassName().equals(WALConstants.LEASE_EXCEPTION_CLASS_NAME)) {
                    FSWAL.log.info("Cannot acquire lease on WAL {}", logFile);
                    try {
                        Thread.sleep(sleepIntervalMs);
                    }
                    catch (InterruptedException ie) {
                        throw new ConnectException((Throwable)ie);
                    }
                    sleepIntervalMs *= 2L;
                    continue;
                }
                throw new ConnectException((Throwable)e);
            }
            catch (IOException e2) {
                throw new DataException("Error creating writer for log file " + this.logFile, (Throwable)e2);
            }
            break;
        }
        if (sleepIntervalMs >= 16000L) {
            throw new ConnectException("Cannot acquire lease after timeout, will retry.");
        }
    }
    
    public void apply() throws ConnectException {
        try {
            if (!this.storage.exists(this.logFile)) {
                return;
            }
            this.acquireLease();
            if (this.reader == null) {
                this.reader = new WALFile.Reader(this.conf.getHadoopConfiguration(), new WALFile.Reader.Option[] { WALFile.Reader.file(new Path(this.logFile)) });
            }
            final Map<WALEntry, WALEntry> entries = new HashMap<WALEntry, WALEntry>();
            final WALEntry key = new WALEntry();
            final WALEntry value = new WALEntry();
            while (this.reader.next((Writable)key, (Writable)value)) {
                final String keyName = key.getName();
                if (keyName.equals(beginMarker)) {
                    entries.clear();
                }
                else if (keyName.equals(endMarker)) {
                    for (final Map.Entry<WALEntry, WALEntry> entry : entries.entrySet()) {
                        final String tempFile = entry.getKey().getName();
                        final String committedFile = entry.getValue().getName();
                        if (!this.storage.exists(committedFile)) {
                            this.storage.commit(tempFile, committedFile);
                        }
                    }
                }
                else {
                    final WALEntry mapKey = new WALEntry(key.getName());
                    final WALEntry mapValue = new WALEntry(value.getName());
                    entries.put(mapKey, mapValue);
                }
            }
        }
        catch (IOException e) {
            FSWAL.log.error("Error applying WAL file: {}, {}", (Object)this.logFile, (Object)e);
            this.close();
            throw new DataException((Throwable)e);
        }
    }
    
    public void truncate() throws ConnectException {
        try {
            final String oldLogFile = this.logFile + ".1";
            this.storage.delete(oldLogFile);
            this.storage.commit(this.logFile, oldLogFile);
        }
        finally {
            this.close();
        }
    }
    
    public void close() throws ConnectException {
        try {
            if (this.writer != null) {
                this.writer.close();
            }
            if (this.reader != null) {
                this.reader.close();
            }
        }
        catch (IOException e) {
            throw new DataException("Error closing " + this.logFile, e);
        }
        finally {
            this.writer = null;
            this.reader = null;
        }
    }
    
    public String getLogFile() {
        return this.logFile;
    }
    
    static {
        log = LoggerFactory.getLogger(FSWAL.class);
    }
}
