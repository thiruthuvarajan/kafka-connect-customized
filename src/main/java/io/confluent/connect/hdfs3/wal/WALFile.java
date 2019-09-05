// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.wal;

import org.apache.hadoop.fs.ChecksumException;
import java.util.Arrays;
import java.io.DataInput;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.Writable;
import java.io.InputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import java.io.DataInputStream;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.OutputStream;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.util.Options;
import org.apache.commons.io.Charsets;
import java.rmi.server.UID;
import org.apache.hadoop.util.Time;
import java.security.MessageDigest;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.fs.Syncable;
import java.io.Closeable;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import io.confluent.connect.hdfs3.Hdfs3SinkConnectorConfig;
import org.apache.commons.logging.Log;

public class WALFile
{
    private static final Log log;
    private static final byte INITIAL_VERSION = 0;
    private static final int SYNC_ESCAPE = -1;
    private static final int SYNC_HASH_SIZE = 16;
    private static final int SYNC_SIZE = 20;
    public static final int SYNC_INTERVAL = 2000;
    private static byte[] VERSION;
    private static String deserErrorFmt;
    
    private WALFile() {
    }
    
    public static Writer createWriter(final Hdfs3SinkConnectorConfig conf, final Writer.Option... opts) throws IOException {
        return new Writer(conf, opts);
    }
    
    private static int getBufferSize(final Configuration conf) {
        return conf.getInt("io.file.buffer.size", 4096);
    }
    
    static {
        log = LogFactory.getLog(WALFile.class);
        WALFile.VERSION = new byte[] { (byte) 'W', (byte) 'A', (byte) 'L', INITIAL_VERSION};
        WALFile.deserErrorFmt = "Could not find a deserializer for the %s class: '%s'. Please ensure that the configuration '%s' is properly configured, if you're using custom serialization.";
    }
    
    public static class Writer implements Closeable, Syncable
    {
        protected Serializer<WALEntry> keySerializer;
        protected Serializer<WALEntry> valSerializer;
        boolean ownOutputStream;
        long lastSyncPos;
        byte[] sync;
        private FSDataOutputStream out;
        private DataOutputBuffer buffer;
        private boolean appendMode;
        
        Writer(final Hdfs3SinkConnectorConfig connectorConfig, final Option... opts) throws IOException {
            this.ownOutputStream = true;
            this.buffer = new DataOutputBuffer();
            try {
                final MessageDigest digester = MessageDigest.getInstance("MD5");
                final long time = Time.now();
                digester.update((new UID() + "@" + time).getBytes(Charsets.UTF_8));
                this.sync = digester.digest();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            Configuration conf = connectorConfig.getHadoopConfiguration();
            BlockSizeOption blockSizeOption = Options.getOption(BlockSizeOption.class, opts);
            BufferSizeOption bufferSizeOption = Options.getOption(BufferSizeOption.class, opts);
            ReplicationOption replicationOption = Options.getOption(ReplicationOption.class, opts);
            FileOption fileOption = Options.getOption(FileOption.class, opts);
            AppendIfExistsOption appendIfExistsOption = Options.getOption(AppendIfExistsOption.class, opts);
            StreamOption streamOption = Options.getOption(StreamOption.class, opts);
            if (fileOption == null == (streamOption == null)) {
                throw new IllegalArgumentException("file or stream must be specified");
            }
            if (fileOption == null && (blockSizeOption != null || bufferSizeOption != null || replicationOption != null)) {
                throw new IllegalArgumentException("file modifier options not compatible with stream");
            }
            FileSystem fs = null;
            boolean ownStream = fileOption != null;
            try {
                FSDataOutputStream out;
                if (ownStream) {
                    Path p = fileOption.getValue();
                    fs = p.getFileSystem(conf);
                    int bufferSize = (bufferSizeOption == null) ? getBufferSize(conf) : bufferSizeOption.getValue();
                    short replication = (replicationOption == null) ? fs.getDefaultReplication(p) : ((short)replicationOption.getValue());
                    long blockSize = (blockSizeOption == null) ? fs.getDefaultBlockSize(p) : blockSizeOption.getValue();
                    if (appendIfExistsOption != null && appendIfExistsOption.getValue() && fs.exists(p) && this.hasIntactVersionHeader(p, fs)) {
                        try (Reader reader = new Reader(connectorConfig.getHadoopConfiguration(), new Reader.Option[] { Reader.file(p), new Reader.OnlyHeaderOption() })) {
                            if (reader.getVersion() != WALFile.VERSION[3]) {
                                throw new VersionMismatchException(WALFile.VERSION[3], reader.getVersion());
                            }
                            this.sync = reader.getSync();
                        }
                        out = fs.append(p, bufferSize);
                        this.appendMode = true;
                    }
                    else {
                        out = fs.create(p, true, bufferSize, replication, blockSize);
                    }
                }
                else {
                    out = streamOption.getValue();
                }
                this.init(connectorConfig, out, ownStream);
            }
            catch (RemoteException re) {
                WALFile.log.error(("Failed creating a WAL Writer: " + re.getMessage()));
                if (re.getClassName().equals(WALConstants.LEASE_EXCEPTION_CLASS_NAME) && fs != null) {
                    fs.close();
                }
                throw re;
            }
        }
        
        private boolean hasIntactVersionHeader(final Path p, final FileSystem fs) throws IOException {
            final FileStatus[] statuses = fs.listStatus(p);
            if (statuses.length != 1) {
                throw new ConnectException("Expected exactly one log for WAL file " + p);
            }
            final boolean result = statuses[0].getLen() >= WALFile.VERSION.length;
            if (!result) {
                WALFile.log.warn((Object)("Failed to read version header from WAL file " + p));
            }
            return result;
        }
        
        public static Option file(final Path value) {
            return new FileOption(value);
        }
        
        public static Option bufferSize(final int value) {
            return new BufferSizeOption(value);
        }
        
        public static Option stream(final FSDataOutputStream value) {
            return new StreamOption(value);
        }
        
        public static Option replication(final short value) {
            return new ReplicationOption(value);
        }
        
        public static Option appendIfExists(final boolean value) {
            return new AppendIfExistsOption(value);
        }
        
        public static Option blockSize(final long value) {
            return new BlockSizeOption(value);
        }
        
        void init(final Hdfs3SinkConnectorConfig connectorConfig, final FSDataOutputStream out, final boolean ownStream) throws IOException {
            final Configuration conf = connectorConfig.getHadoopConfiguration();
            this.out = out;
            this.ownOutputStream = ownStream;
            final SerializationFactory serializationFactory = new SerializationFactory(conf);
            this.keySerializer = serializationFactory.getSerializer(WALEntry.class);
            if (this.keySerializer == null) {
                final String errorMsg = String.format(WALFile.deserErrorFmt, "Key", WALEntry.class.getCanonicalName(), "io.serializations");
                throw new IOException(errorMsg);
            }
            this.keySerializer.open((OutputStream)this.buffer);
            this.valSerializer = serializationFactory.getSerializer(WALEntry.class);
            if (this.valSerializer == null) {
                final String errorMsg = String.format(WALFile.deserErrorFmt, "Value", WALEntry.class.getCanonicalName(), "io.serializations");
                throw new IOException(errorMsg);
            }
            this.valSerializer.open((OutputStream)this.buffer);
            if (this.appendMode) {
                this.sync();
            }
            else {
                this.writeFileHeader();
            }
        }
        
        public synchronized void append(WALEntry key, WALEntry val) throws IOException {
            this.buffer.reset();
            this.keySerializer.serialize(key);
            final int keyLength = this.buffer.getLength();
            if (keyLength < 0) {
                throw new IOException("negative length keys not allowed: " + key);
            }
            this.valSerializer.serialize(val);
            this.checkAndWriteSync();
            this.out.writeInt(this.buffer.getLength());
            this.out.writeInt(keyLength);
            this.out.write(this.buffer.getData(), 0, this.buffer.getLength());
        }
        
        public synchronized long getLength() throws IOException {
            return this.out.getPos();
        }
        
        private synchronized void checkAndWriteSync() throws IOException {
            if (this.sync != null && this.out.getPos() >= this.lastSyncPos + 2000L) {
                this.sync();
            }
        }
        
        private void writeFileHeader() throws IOException {
            this.out.write(WALFile.VERSION);
            this.out.write(this.sync);
            this.out.flush();
        }
        
        @Override
        public synchronized void close() throws IOException {
            this.keySerializer.close();
            this.valSerializer.close();
            if (this.out != null) {
                if (this.ownOutputStream) {
                    this.out.close();
                }
                else {
                    this.out.flush();
                }
                this.out = null;
            }
        }
        
        public void sync() throws IOException {
            if (this.sync != null && this.lastSyncPos != this.out.getPos()) {
                this.out.writeInt(SYNC_ESCAPE);
                this.out.write(this.sync);
                this.lastSyncPos = this.out.getPos();
            }
        }
        
        public void hflush() throws IOException {
            if (this.out != null) {
                this.out.hflush();
            }
        }
        
        public void hsync() throws IOException {
            if (this.out != null) {
                this.out.hsync();
            }
        }
        
        static class FileOption extends Options.PathOption implements Option
        {
            FileOption(final Path path) {
                super(path);
            }
        }
        
        static class StreamOption extends Options.FSDataOutputStreamOption implements Option
        {
            StreamOption(final FSDataOutputStream stream) {
                super(stream);
            }
        }
        
        static class BufferSizeOption extends Options.IntegerOption implements Option
        {
            BufferSizeOption(final int value) {
                super(value);
            }
        }
        
        static class BlockSizeOption extends Options.LongOption implements Option
        {
            BlockSizeOption(final long value) {
                super(value);
            }
        }
        
        static class ReplicationOption extends Options.IntegerOption implements Option
        {
            ReplicationOption(final int value) {
                super(value);
            }
        }
        
        static class AppendIfExistsOption extends Options.BooleanOption implements Option
        {
            AppendIfExistsOption(final boolean value) {
                super(value);
            }
        }
        
        public interface Option
        {
        }
    }
    
    public static class Reader implements Closeable
    {
        private String filename;
        private FSDataInputStream in;
        private DataOutputBuffer outBuf;
        private byte version;
        private byte[] sync;
        private byte[] syncCheck;
        private boolean syncSeen;
        private long headerEnd;
        private long end;
        private int keyLength;
        private int recordLength;
        private Configuration conf;
        private DataInputBuffer valBuffer;
        private DataInputStream valIn;
        private Deserializer<WALEntry> keyDeserializer;
        private Deserializer<WALEntry> valDeserializer;
        
        public Reader(final Configuration conf, final Option... opts) throws IOException {
            this.outBuf = new DataOutputBuffer();
            this.sync = new byte[SYNC_HASH_SIZE];
            this.syncCheck = new byte[SYNC_HASH_SIZE];
            this.valBuffer = null;
            this.valIn = null;
            FileOption fileOpt = Options.getOption(FileOption.class, opts);
            InputStreamOption streamOpt = Options.getOption(InputStreamOption.class, opts);
            LengthOption lenOpt = Options.getOption(LengthOption.class, opts);
            BufferSizeOption bufOpt = Options.getOption(BufferSizeOption.class, opts);
            if (fileOpt == null == (streamOpt == null)) {
                throw new IllegalArgumentException("File or stream option must be specified");
            }
            if (fileOpt == null && bufOpt != null) {
                throw new IllegalArgumentException("buffer size can only be set when a file is specified.");
            }
            Path filename = null;
            FileSystem fs = null;
            try {
                long len;
                FSDataInputStream file;
                if (fileOpt != null) {
                    filename = fileOpt.getValue();
                    fs = filename.getFileSystem(conf);
                    final int bufSize = (bufOpt == null) ? getBufferSize(conf) : bufOpt.getValue();
                    len = ((null == lenOpt) ? fs.getFileStatus(filename).getLen() : lenOpt.getValue());
                    file = this.openFile(fs, filename, bufSize, len);
                }
                else {
                    len = ((null == lenOpt) ? Long.MAX_VALUE : lenOpt.getValue());
                    file = streamOpt.getValue();
                }
                final StartOption startOpt = Options.getOption(StartOption.class, (Object[])opts);
                final long start = (startOpt == null) ? 0L : startOpt.getValue();
                final OnlyHeaderOption headerOnly = Options.getOption(OnlyHeaderOption.class, opts);
                this.initialize(filename, file, start, len, conf, headerOnly != null);
            }
            catch (RemoteException re) {
                WALFile.log.error(("Failed creating a WAL Reader: " + re.getMessage()));
                if (re.getClassName().equals(WALConstants.LEASE_EXCEPTION_CLASS_NAME) && fs != null) {
                    fs.close();
                }
                throw re;
            }
        }
        
        public static Option file(Path value) {
            return new FileOption(value);
        }
        
        public static Option stream(FSDataInputStream value) {
            return new InputStreamOption(value);
        }
        
        public static Option start(long value) {
            return new StartOption(value);
        }
        
        public static Option length(long value) {
            return new LengthOption(value);
        }
        
        public static Option bufferSize(int value) {
            return new BufferSizeOption(value);
        }
        
        private void initialize(final Path filename, final FSDataInputStream in, final long start, final long length, final Configuration conf, final boolean tempReader) throws IOException {
            if (in == null) {
                throw new IllegalArgumentException("in == null");
            }
            this.filename = ((filename == null) ? "<unknown>" : filename.toString());
            this.in = in;
            this.conf = conf;
            boolean succeeded = false;
            try {
                this.seek(start);
                this.end = this.in.getPos() + length;
                if (this.end < length) {
                    this.end = Long.MAX_VALUE;
                }
                this.init(tempReader);
                succeeded = true;
            }
            finally {
                if (!succeeded) {
                    IOUtils.cleanup(WALFile.log, new Closeable[] { (Closeable)this.in });
                }
            }
        }
        
        protected FSDataInputStream openFile(final FileSystem fs, final Path file, final int bufferSize, final long length) throws IOException {
            return fs.open(file, bufferSize);
        }
        
        private void init(final boolean tempReader) throws IOException {
            final byte[] versionBlock = new byte[WALFile.VERSION.length];
            this.in.readFully(versionBlock);
            if (versionBlock[0] != WALFile.VERSION[0] || versionBlock[1] != WALFile.VERSION[1] || versionBlock[2] != WALFile.VERSION[2]) {
                throw new IOException(this + " not a WALFile");
            }
            this.version = versionBlock[3];
            if (this.version > WALFile.VERSION[3]) {
                throw new VersionMismatchException(WALFile.VERSION[3], this.version);
            }
            this.in.readFully(this.sync);
            this.headerEnd = this.in.getPos();
            if (!tempReader) {
                this.valBuffer = new DataInputBuffer();
                this.valIn = (DataInputStream)this.valBuffer;
                final SerializationFactory serializationFactory = new SerializationFactory(this.conf);
                this.keyDeserializer = this.getDeserializer(serializationFactory, WALEntry.class);
                if (this.keyDeserializer == null) {
                    final String errorMsg = String.format(WALFile.deserErrorFmt, "Key", WALEntry.class.getCanonicalName(), "io.serializations");
                    throw new IOException(errorMsg);
                }
                this.keyDeserializer.open((InputStream)this.valBuffer);
                this.valDeserializer = this.getDeserializer(serializationFactory, WALEntry.class);
                if (this.valDeserializer == null) {
                    final String errorMsg = String.format(WALFile.deserErrorFmt, "Value", WALEntry.class.getCanonicalName(), "io.serializations");
                    throw new IOException(errorMsg);
                }
                this.valDeserializer.open((InputStream)this.valIn);
            }
        }
        
        private <T> Deserializer<T> getDeserializer(SerializationFactory sf, Class<T> c) {
            return sf.getDeserializer(c);
        }
        
        private byte[] getSync() {
            return this.sync;
        }
        
        @Override
        public synchronized void close() throws IOException {
            if (this.keyDeserializer != null) {
                this.keyDeserializer.close();
            }
            if (this.valDeserializer != null) {
                this.valDeserializer.close();
            }
            this.in.close();
        }
        
        private byte getVersion() {
            return this.version;
        }
        
        Configuration getConf() {
            return this.conf;
        }
        
        private synchronized void seekToCurrentValue() throws IOException {
            this.valBuffer.reset();
        }
        
        public synchronized void getCurrentValue(final Writable val) throws IOException {
            if (val instanceof Configurable) {
                ((Configurable)val).setConf(this.conf);
            }
            this.seekToCurrentValue();
            val.readFields((DataInput)this.valIn);
            if (this.valIn.read() > 0) {
                WALFile.log.info((Object)("available bytes: " + this.valIn.available()));
                throw new IOException(val + " read " + (this.valBuffer.getPosition() - this.keyLength) + " bytes, should read " + (this.valBuffer.getLength() - this.keyLength));
            }
        }
        
        public synchronized WALEntry getCurrentValue(WALEntry val) throws IOException {
            if (val instanceof Configurable) {
                ((Configurable)val).setConf(this.conf);
            }
            this.seekToCurrentValue();
            val = this.deserializeValue(val);
            if (this.valIn.read() > 0) {
                WALFile.log.info((Object)("available bytes: " + this.valIn.available()));
                throw new IOException(val + " read " + (this.valBuffer.getPosition() - this.keyLength) + " bytes, should read " + (this.valBuffer.getLength() - this.keyLength));
            }
            return val;
        }
        
        private WALEntry deserializeValue(final WALEntry val) throws IOException {
            return valDeserializer.deserialize(val);
        }
        
        private synchronized int readRecordLength() throws IOException {
            if (this.in.getPos() >= this.end) {
                return -1;
            }
            int length = this.in.readInt();
            if (this.sync != null && length == -1) {
                this.in.readFully(this.syncCheck);
                if (!Arrays.equals(this.sync, this.syncCheck)) {
                    throw new IOException("File is corrupt!");
                }
                this.syncSeen = true;
                if (this.in.getPos() >= this.end) {
                    return -1;
                }
                length = this.in.readInt();
            }
            else {
                this.syncSeen = false;
            }
            return length;
        }
        
        public synchronized boolean next(final Writable key) throws IOException {
            if (key.getClass() != WALEntry.class) {
                throw new IOException("wrong key class: " + key.getClass().getName() + " is not " + WALEntry.class);
            }
            this.outBuf.reset();
            this.keyLength = this.next(this.outBuf);
            if (this.keyLength < 0) {
                return false;
            }
            this.valBuffer.reset(this.outBuf.getData(), this.outBuf.getLength());
            key.readFields((DataInput)this.valBuffer);
            this.valBuffer.mark(0);
            if (this.valBuffer.getPosition() != this.keyLength) {
                throw new IOException(key + " read " + this.valBuffer.getPosition() + " bytes, should read " + this.keyLength);
            }
            return true;
        }
        
        public synchronized boolean next(final Writable key, final Writable val) throws IOException {
            if (val.getClass() != WALEntry.class) {
                throw new IOException("wrong value class: " + val + " is not " + WALEntry.class);
            }
            final boolean more = this.next(key);
            if (more) {
                this.getCurrentValue(val);
            }
            return more;
        }
        
        synchronized int next(final DataOutputBuffer buffer) throws IOException {
            try {
                final int length = this.readRecordLength();
                if (length == -1) {
                    return -1;
                }
                final int keyLength = this.in.readInt();
                buffer.write((DataInput)this.in, length);
                return keyLength;
            }
            catch (ChecksumException e) {
                this.handleChecksumException(e);
                return this.next(buffer);
            }
        }
        
        public synchronized WALEntry next(WALEntry key) throws IOException {
            this.outBuf.reset();
            this.keyLength = this.next(this.outBuf);
            if (this.keyLength < 0) {
                return null;
            }
            this.valBuffer.reset(this.outBuf.getData(), this.outBuf.getLength());
            key = this.deserializeKey(key);
            this.valBuffer.mark(0);
            if (this.valBuffer.getPosition() != this.keyLength) {
                throw new IOException(key + " read " + this.valBuffer.getPosition() + " bytes, should read " + this.keyLength);
            }
            return key;
        }
        
        private WALEntry deserializeKey(WALEntry key) throws IOException {
            return keyDeserializer.deserialize(key);
        }
        
        private void handleChecksumException(final ChecksumException e) throws IOException {
            if (this.conf.getBoolean("io.skip.checksum.errors", false)) {
                WALFile.log.warn((Object)("Bad checksum at " + this.getPosition() + ". Skipping entries."));
                this.sync(this.getPosition() + this.conf.getInt("io.bytes.per.checksum", 512));
                return;
            }
            throw e;
        }
        
        synchronized void ignoreSync() {
            this.sync = null;
        }
        
        public synchronized void seek(final long position) throws IOException {
            this.in.seek(position);
        }
        
        public synchronized void sync(final long position) throws IOException {
            if (position + SYNC_SIZE >= this.end) {
                this.seek(this.end);
                return;
            }
            if (position < this.headerEnd) {
                this.in.seek(this.headerEnd);
                this.syncSeen = true;
                return;
            }
            try {
                this.seek(position + 4L);
                this.in.readFully(this.syncCheck);
                final int syncLen = this.sync.length;
                int i = 0;
                while (this.in.getPos() < this.end) {
                    int j;
                    for (j = 0; j < syncLen && this.sync[j] == this.syncCheck[(i + j) % syncLen]; ++j) {}
                    if (j == syncLen) {
                        this.in.seek(this.in.getPos() - 20L);
                        return;
                    }
                    this.syncCheck[i % syncLen] = this.in.readByte();
                    ++i;
                }
            }
            catch (ChecksumException e) {
                this.handleChecksumException(e);
            }
        }
        
        public synchronized boolean syncSeen() {
            return this.syncSeen;
        }
        
        public synchronized long getPosition() throws IOException {
            return this.in.getPos();
        }
        
        @Override
        public String toString() {
            return this.filename;
        }
        
        private static class FileOption extends Options.PathOption implements Option
        {
            private FileOption(final Path value) {
                super(value);
            }
        }
        
        private static class InputStreamOption extends Options.FSDataInputStreamOption implements Option
        {
            private InputStreamOption(final FSDataInputStream value) {
                super(value);
            }
        }
        
        private static class StartOption extends Options.LongOption implements Option
        {
            private StartOption(final long value) {
                super(value);
            }
        }
        
        private static class LengthOption extends Options.LongOption implements Option
        {
            private LengthOption(final long value) {
                super(value);
            }
        }
        
        private static class BufferSizeOption extends Options.IntegerOption implements Option
        {
            private BufferSizeOption(final int value) {
                super(value);
            }
        }
        
        private static class OnlyHeaderOption extends Options.BooleanOption implements Option
        {
            private OnlyHeaderOption() {
                super(true);
            }
        }
        
        public interface Option
        {
        }
    }
}
