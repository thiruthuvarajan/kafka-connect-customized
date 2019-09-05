// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3;

import org.slf4j.LoggerFactory;
import java.util.concurrent.Callable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.kafka.connect.errors.DataException;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileStatus;
import io.confluent.connect.hdfs3.filter.CommittedFileFilter;
import io.confluent.connect.storage.errors.HiveMetaStoreException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import io.confluent.connect.hdfs3.filter.TopicPartitionCommittedFileFilter;
import org.joda.time.DateTime;
import io.confluent.connect.storage.util.DateTimeUtils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedList;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import io.confluent.connect.storage.hive.HiveUtil;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.hive.HiveMetaStore;
import org.joda.time.DateTimeZone;
import org.apache.kafka.connect.data.Schema;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import java.util.Set;
import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.sink.SinkRecord;
import java.util.Queue;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

import org.apache.kafka.common.TopicPartition;
import io.confluent.connect.storage.format.RecordWriter;
import java.util.Map;
import io.confluent.connect.storage.wal.WAL;
import io.confluent.connect.hdfs3.storage.HdfsStorage;
import io.confluent.common.utils.Time;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import org.slf4j.Logger;

public class TopicPartitionWriter
{
	private static final Logger log;
	private static final TimestampExtractor WALLCLOCK;
	private final RecordWriterProvider<Hdfs3SinkConnectorConfig> newWriterProvider;
	private final String zeroPadOffsetFormat;
	private final boolean hiveIntegration;
	private final Time time;
	private final HdfsStorage storage;
	private final WAL wal;
	private final Map<String, String> tempFiles;
	private final Map<String, RecordWriter> writers;
	private final TopicPartition tp;
	private final Partitioner<FieldSchema> partitioner;
	private final TimestampExtractor timestampExtractor;
	private final boolean isWallclockBased;
	private final String url;
	private final String topicsDir;
	private State state;
	private final Queue<SinkRecord> buffer;
	private boolean recovered;
	private final SinkTaskContext context;
	private int recordCounter;
	private final int flushSize;
	private final long rotateIntervalMs;
	private Long lastRotate;
	private final long rotateScheduleIntervalMs;
	private long nextScheduledRotate;
	private final io.confluent.connect.hdfs3.RecordWriterProvider writerProvider;
	private final Hdfs3SinkConnectorConfig connectorConfig;
	private final AvroData avroData;
	private final Set<String> appended;
	private long offset;
	private final Map<String, Long> startOffsets;
	private final Map<String, Long> offsets;
	private final long timeoutMs;
	private long failureTime;
	private final StorageSchemaCompatibility compatibility;
	private Schema currentSchema;
	private final String extension;
	private final DateTimeZone timeZone;
	private final String hiveDatabase;
	private final HiveMetaStore hiveMetaStore;
	private final SchemaFileReader<Hdfs3SinkConnectorConfig, Path> schemaFileReader;
	private final HiveUtil hive;
	private final ExecutorService executorService;
	private final Queue<Future<Void>> hiveUpdateFutures;
	private final Set<String> hivePartitions;

	public TopicPartitionWriter(final TopicPartition tp, final HdfsStorage storage, final io.confluent.connect.hdfs3.RecordWriterProvider writerProvider, final RecordWriterProvider<Hdfs3SinkConnectorConfig> newWriterProvider, final Partitioner<FieldSchema> partitioner, final Hdfs3SinkConnectorConfig connectorConfig, final SinkTaskContext context, final AvroData avroData, final Time time) {
		this(tp, (Storage<Hdfs3SinkConnectorConfig, ?>)storage, writerProvider, newWriterProvider, partitioner, connectorConfig, context, avroData, null, null, null, null, null, time);
	}

	public TopicPartitionWriter(TopicPartition tp, 
			Storage<Hdfs3SinkConnectorConfig, ?> storage, final io.confluent.connect.hdfs3.RecordWriterProvider writerProvider, final RecordWriterProvider<Hdfs3SinkConnectorConfig> newWriterProvider, final Partitioner<FieldSchema> partitioner, final Hdfs3SinkConnectorConfig connectorConfig, final SinkTaskContext context, final AvroData avroData, final HiveMetaStore hiveMetaStore, final HiveUtil hive, final SchemaFileReader<Hdfs3SinkConnectorConfig, Path> schemaFileReader, final ExecutorService executorService, final Queue<Future<Void>> hiveUpdateFutures, final Time time) {
		this.time = time;
		this.tp = tp;
		this.context = context;
		this.avroData = avroData;
		this.storage = (HdfsStorage) storage;
		this.writerProvider = writerProvider;
		this.newWriterProvider = newWriterProvider;
		this.partitioner = partitioner;
		TimestampExtractor timestampExtractor = null;
		if (partitioner instanceof DataWriter.PartitionerWrapper) {
			final Partitioner<?> inner = ((DataWriter.PartitionerWrapper)partitioner).partitioner;
			if (TimeBasedPartitioner.class.isAssignableFrom(inner.getClass())) {
				timestampExtractor = ((TimeBasedPartitioner) inner).getTimestampExtractor();
			}
		}
		this.timestampExtractor = ((timestampExtractor != null) ? timestampExtractor : TopicPartitionWriter.WALLCLOCK);
		this.isWallclockBased = TimeBasedPartitioner.WallclockTimestampExtractor.class.isAssignableFrom(this.timestampExtractor.getClass());
		this.url = storage.url();
		this.connectorConfig = this.storage.conf();
		this.schemaFileReader = schemaFileReader;
		
		this.topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
		this.flushSize = connectorConfig.getInt(Hdfs3SinkConnectorConfig.FLUSH_SIZE_CONFIG);
		this.rotateIntervalMs = connectorConfig.getLong(Hdfs3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG);
		this.rotateScheduleIntervalMs = connectorConfig.getLong(Hdfs3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
		this.timeoutMs = connectorConfig.getLong(Hdfs3SinkConnectorConfig.RETRY_BACKOFF_CONFIG);
		this.compatibility = StorageSchemaCompatibility.getCompatibility(connectorConfig.getString(StorageSinkConnectorConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG));
		final String logsDir = connectorConfig.getString(Hdfs3SinkConnectorConfig.LOGS_DIR_CONFIG);
		this.wal = this.storage.wal(logsDir, tp);
		this.buffer = new LinkedList<SinkRecord>();
		this.writers = new HashMap<String, RecordWriter>();
		this.tempFiles = new HashMap<String, String>();
		this.appended = new HashSet<String>();
		this.startOffsets = new HashMap<String, Long>();
		this.offsets = new HashMap<String, Long>();
		this.state = State.RECOVERY_STARTED;
		this.failureTime = -1L;
		this.offset = -1L;
		if (writerProvider != null) {
			this.extension = writerProvider.getExtension();
		}
		else {
			if (newWriterProvider == null) {
				throw new ConnectException("Invalid state: either old or new RecordWriterProvider must be provided");
			}
			this.extension = newWriterProvider.getExtension();
		}
		this.zeroPadOffsetFormat = "%0" + connectorConfig.getInt(Hdfs3SinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG) + "d";
		this.hiveIntegration = connectorConfig.getBoolean(HiveConfig.HIVE_INTEGRATION_CONFIG);
		if (this.hiveIntegration) {
			this.hiveDatabase = connectorConfig.getString(HiveConfig.HIVE_DATABASE_CONFIG);
		}
		else {
			this.hiveDatabase = null;
		}
		this.hiveMetaStore = hiveMetaStore;
		this.hive = hive;
		this.executorService = executorService;
		this.hiveUpdateFutures = hiveUpdateFutures;
		this.hivePartitions = new HashSet<String>();
		if (rotateScheduleIntervalMs > 0) {
			this.timeZone = DateTimeZone.forID(connectorConfig.getString(PartitionerConfig.TIMEZONE_CONFIG));
		}
		else {
			this.timeZone = null;
		}
		this.updateRotationTimers(null);
	}

	@SuppressWarnings("fallthrough")
	public boolean recover() {
		try {
			switch (state) {
			case RECOVERY_STARTED: 
				TopicPartitionWriter.log.info("Started recovery for topic partition {}", tp);
				this.pause();
				this.nextState();

			case RECOVERY_PARTITION_PAUSED: 
				this.applyWAL();
				this.nextState();

			case WAL_APPLIED: 
				this.truncateWAL();
				this.nextState();

			case WAL_TRUNCATED: 
				this.resetOffsets();
				this.nextState();

			case OFFSET_RESET: 
				this.resume();
				this.nextState();
				TopicPartitionWriter.log.info("Finished recovery for topic partition {}", tp);
				break;

			default: 
				TopicPartitionWriter.log.error("{} is not a valid state to perform recovery for topic partition {}.", (Object)this.state, (Object)this.tp);
				break;

			}
		}
		catch (ConnectException e) {
			TopicPartitionWriter.log.error("Recovery failed at state {}", (Object)this.state, (Object)e);
			this.setRetryTimeout(this.timeoutMs);
			return false;
		}
		return true;
	}

	private void updateRotationTimers(final SinkRecord currentRecord) {
		final long now = this.time.milliseconds();
		this.lastRotate = (this.isWallclockBased ? Long.valueOf(now) : ((currentRecord != null) ? this.timestampExtractor.extract((ConnectRecord)currentRecord) : null));
		if (TopicPartitionWriter.log.isDebugEnabled() && this.rotateIntervalMs > 0L) {
			TopicPartitionWriter.log.debug("Update last rotation timer. Next rotation for {} will be in {}ms", (Object)this.tp, (Object)this.rotateIntervalMs);
		}
		if (this.rotateScheduleIntervalMs > 0L) {
			this.nextScheduledRotate = DateTimeUtils.getNextTimeAdjustedByDay(now, this.rotateScheduleIntervalMs, this.timeZone);
			if (TopicPartitionWriter.log.isDebugEnabled()) {
				TopicPartitionWriter.log.debug("Update scheduled rotation timer. Next rotation for {} will be at {}", (Object)this.tp, (Object)new DateTime(this.nextScheduledRotate).withZone(this.timeZone).toString());
			}
		}
	}
	
	
	@SuppressWarnings("fallthrough")
	public void write() {
		final long now = this.time.milliseconds();
		SinkRecord currentRecord = null;
		if (this.failureTime > 0L && now - this.failureTime < this.timeoutMs) {
			return;
		}
		if (this.state.compareTo(State.WRITE_STARTED) < 0) {
			final boolean success = this.recover();
			if (!success) {
				return;
			}
			this.updateRotationTimers(null);
		}
		ConnectException e = null;
		while (!this.buffer.isEmpty()) {
			try {
				Label_0425: {
				switch (state) {
				case WRITE_STARTED: {
					pause();
					nextState();
				}
				case WRITE_PARTITION_PAUSED: {
					if (this.currentSchema == null && this.compatibility != StorageSchemaCompatibility.NONE && this.offset != -1L) {
						final String topicDir = FileUtils.topicDirectory(this.url, this.topicsDir, this.tp.topic());
						final CommittedFileFilter filter = new TopicPartitionCommittedFileFilter(this.tp);
						final FileStatus fileStatusWithMaxOffset = FileUtils.fileStatusWithMaxOffset(this.storage, new Path(topicDir), filter);
						if (fileStatusWithMaxOffset != null) {
							this.currentSchema = this.schemaFileReader.getSchema(this.connectorConfig, fileStatusWithMaxOffset.getPath());
						}
					}
					final SinkRecord record = currentRecord = this.buffer.peek();
					final Schema valueSchema = record.valueSchema();
					if ((this.recordCounter <= 0 && this.currentSchema == null && valueSchema != null) || this.compatibility.shouldChangeSchema((ConnectRecord)record, (Schema)null, this.currentSchema)) {
						this.currentSchema = valueSchema;
						if (this.hiveIntegration) {
							this.createHiveTable();
							this.alterHiveSchema();
						}
						if (this.recordCounter > 0) {
							this.nextState();
							break Label_0425;
						}
						continue;
					}
					else {
						if (this.shouldRotateAndMaybeUpdateTimers(currentRecord, now)) {
							TopicPartitionWriter.log.info("Starting commit and rotation for topic partition {} with start offsets {} and end offsets {}", new Object[] { this.tp, this.startOffsets, this.offsets });
							this.nextState();
							break Label_0425;
						}
						SinkRecord projectedRecord = this.compatibility.project(record, (Schema)null, this.currentSchema);
						writeRecord(projectedRecord);
						buffer.poll();
						break;
					}

				}
				case SHOULD_ROTATE: {
					updateRotationTimers(currentRecord);
					closeTempFile();
					nextState();
				}
				case TEMP_FILE_CLOSED: {
					this.appendToWAL();
					this.nextState();
				}
				case WAL_APPENDED: {
					this.commitFile();
					this.nextState();
				}
				case FILE_COMMITTED: {
					this.setState(State.WRITE_PARTITION_PAUSED);
					continue;
				}
				default: {
					TopicPartitionWriter.log.error("{} is not a valid state to write record for topic partition {}.", (Object)this.state, (Object)this.tp);
					continue;
				}
				}
			}
			continue;
			}
			catch (SchemaProjectorException | IllegalWorkerStateException | HiveMetaStoreException ex2) {
				final ConnectException ex = ex2;
				throw new RuntimeException((Throwable)ex);
			}
			catch (ConnectException e2) {
				TopicPartitionWriter.log.error("Exception on topic partition {}: ", (Object)this.tp, (Object)e2);
				this.failureTime = this.time.milliseconds();
				this.setRetryTimeout(this.timeoutMs);
			}
			break;
		}
		if (this.buffer.isEmpty()) {
			if (this.recordCounter > 0 && this.shouldRotateAndMaybeUpdateTimers(currentRecord, now)) {
				TopicPartitionWriter.log.info("committing files after waiting for rotateIntervalMs time but less than flush.size records available.");
				this.updateRotationTimers(currentRecord);
				try {
					this.closeTempFile();
					this.appendToWAL();
					this.commitFile();
				}
				catch (ConnectException e3) {
					TopicPartitionWriter.log.error("Exception on topic partition {}: ", (Object)this.tp, (Object)e3);
					this.failureTime = this.time.milliseconds();
					this.setRetryTimeout(this.timeoutMs);
				}
			}
			this.resume();
			this.state = State.WRITE_STARTED;
		}
	}

	public void close() throws ConnectException {
		TopicPartitionWriter.log.debug("Closing TopicPartitionWriter {}", (Object)this.tp);
		final List<Exception> exceptions = new ArrayList<Exception>();
		for (final String encodedPartition : this.tempFiles.keySet()) {
			try {
				if (!this.writers.containsKey(encodedPartition)) {
					continue;
				}
				TopicPartitionWriter.log.debug("Discarding in progress tempfile {} for {} {}", new Object[] { this.tempFiles.get(encodedPartition), this.tp, encodedPartition });
				this.closeTempFile(encodedPartition);
				this.deleteTempFile(encodedPartition);
			}
			catch (DataException e) {
				TopicPartitionWriter.log.error("Error discarding temp file {} for {} {} when closing TopicPartitionWriter:", new Object[] { this.tempFiles.get(encodedPartition), this.tp, encodedPartition, e });
			}
		}
		this.writers.clear();
		try {
			this.wal.close();
		}
		catch (ConnectException e2) {
			TopicPartitionWriter.log.error("Error closing {}.", (Object)this.wal.getLogFile(), (Object)e2);
			exceptions.add((Exception)e2);
		}
		this.startOffsets.clear();
		this.offsets.clear();
		if (exceptions.size() != 0) {
			final StringBuilder sb = new StringBuilder();
			for (final Exception exception : exceptions) {
				sb.append(exception.getMessage());
				sb.append("\n");
			}
			throw new ConnectException("Error closing writer: " + sb.toString());
		}
	}

	public void buffer(final SinkRecord sinkRecord) {
		this.buffer.add(sinkRecord);
	}

	public long offset() {
		return this.offset;
	}

	Map<String, RecordWriter> getWriters() {
		return this.writers;
	}

	public Map<String, String> getTempFiles() {
		return this.tempFiles;
	}

	private String getDirectory(final String encodedPartition) {
		return this.partitioner.generatePartitionedPath(this.tp.topic(), encodedPartition);
	}

	private void nextState() {
		this.state = this.state.next();
	}

	private void setState(final State state) {
		this.state = state;
	}

	private boolean shouldRotateAndMaybeUpdateTimers(final SinkRecord currentRecord, final long now) {
		Long currentTimestamp = null;
		if (this.isWallclockBased) {
			currentTimestamp = now;
		}
		else if (currentRecord != null) {
			currentTimestamp = this.timestampExtractor.extract((ConnectRecord)currentRecord);
			this.lastRotate = ((this.lastRotate == null) ? currentTimestamp : this.lastRotate);
		}
		final boolean periodicRotation = this.rotateIntervalMs > 0L && currentTimestamp != null && this.lastRotate != null && currentTimestamp - this.lastRotate >= this.rotateIntervalMs;
		final boolean scheduledRotation = this.rotateScheduleIntervalMs > 0L && now >= this.nextScheduledRotate;
		final boolean messageSizeRotation = this.recordCounter >= this.flushSize;
		TopicPartitionWriter.log.trace("Should apply periodic time-based rotation (rotateIntervalMs: '{}', lastRotate: '{}', timestamp: '{}')? {}", new Object[] { this.rotateIntervalMs, this.lastRotate, currentTimestamp, periodicRotation });
		TopicPartitionWriter.log.trace("Should apply scheduled rotation: (rotateScheduleIntervalMs: '{}', nextScheduledRotate: '{}', now: '{}')? {}", new Object[] { this.rotateScheduleIntervalMs, this.nextScheduledRotate, now, scheduledRotation });
		TopicPartitionWriter.log.trace("Should apply size-based rotation (count {} >= flush size {})? {}", new Object[] { this.recordCounter, this.flushSize, messageSizeRotation });
		return periodicRotation || scheduledRotation || messageSizeRotation;
	}

	private void readOffset() throws ConnectException {
		final String path = FileUtils.topicDirectory(this.url, this.topicsDir, this.tp.topic());
		final CommittedFileFilter filter = new TopicPartitionCommittedFileFilter(this.tp);
		final FileStatus fileStatusWithMaxOffset = FileUtils.fileStatusWithMaxOffset(this.storage, new Path(path), filter);
		if (fileStatusWithMaxOffset != null) {
			final long lastCommittedOffsetToHdfs = FileUtils.extractOffset(fileStatusWithMaxOffset.getPath().getName());
			this.offset = lastCommittedOffsetToHdfs + 1L;
		}
	}

	private void pause() {
		this.context.pause(new TopicPartition[] { this.tp });
	}

	private void resume() {
		this.context.resume(new TopicPartition[] { this.tp });
	}

	private RecordWriter getWriter(final SinkRecord record, final String encodedPartition) throws ConnectException {
		if (this.writers.containsKey(encodedPartition)) {
			return this.writers.get(encodedPartition);
		}
		final String tempFile = this.getTempFile(encodedPartition);
		RecordWriter writer;
		try {
			if (this.writerProvider != null) {
				writer = (RecordWriter)new OldRecordWriterWrapper(this.writerProvider.getRecordWriter(this.connectorConfig.getHadoopConfiguration(), tempFile, record, this.avroData));
			}
			else {
				if (this.newWriterProvider == null) {
					throw new ConnectException("Invalid state: either old or new RecordWriterProvider must be provided");
				}
				writer = this.newWriterProvider.getRecordWriter(this.connectorConfig, tempFile);
			}
		}
		catch (IOException e) {
			throw new ConnectException("Couldn't create RecordWriter", (Throwable)e);
		}
		this.writers.put(encodedPartition, writer);
		if (this.hiveIntegration && !this.hivePartitions.contains(encodedPartition)) {
			this.addHivePartition(encodedPartition);
			this.hivePartitions.add(encodedPartition);
		}
		return writer;
	}

	private String getTempFile(final String encodedPartition) {
		String tempFile;
		if (this.tempFiles.containsKey(encodedPartition)) {
			tempFile = this.tempFiles.get(encodedPartition);
		}
		else {
			final String directory = "/+tmp/" + this.getDirectory(encodedPartition);
			tempFile = FileUtils.tempFileName(this.url, this.topicsDir, directory, this.extension);
			this.tempFiles.put(encodedPartition, tempFile);
		}
		return tempFile;
	}

	private void applyWAL() throws ConnectException {
		if (!this.recovered) {
			this.wal.apply();
		}
	}

	private void truncateWAL() throws ConnectException {
		if (!this.recovered) {
			this.wal.truncate();
		}
	}

	private void resetOffsets() throws ConnectException {
		if (!this.recovered) {
			this.readOffset();
			if (this.offset > 0L) {
				TopicPartitionWriter.log.debug("Resetting offset for {} to {}", (Object)this.tp, (Object)this.offset);
				this.context.offset(this.tp, this.offset);
			}
			else {
				TopicPartitionWriter.log.debug("Resetting offset for {} based upon existing consumer group offsets or, if there are none, the consumer's 'auto.offset.reset' value.", (Object)this.tp);
			}
			this.recovered = true;
		}
	}

	private void writeRecord(final SinkRecord record) {
		if (this.offset == -1L) {
			this.offset = record.kafkaOffset();
		}
		final String encodedPartition = this.partitioner.encodePartition(record);
		final RecordWriter writer = this.getWriter(record, encodedPartition);
		writer.write(record);
		if (!this.startOffsets.containsKey(encodedPartition)) {
			this.startOffsets.put(encodedPartition, record.kafkaOffset());
		}
		this.offsets.put(encodedPartition, record.kafkaOffset());
		++this.recordCounter;
	}

	private void closeTempFile(final String encodedPartition) {
		if (this.writers.containsKey(encodedPartition)) {
			final RecordWriter writer = this.writers.get(encodedPartition);
			writer.close();
			this.writers.remove(encodedPartition);
		}
	}

	private void closeTempFile() {
		for (final String encodedPartition : this.tempFiles.keySet()) {
			this.closeTempFile(encodedPartition);
		}
	}

	private void appendToWAL(final String encodedPartition) {
		final String tempFile = this.tempFiles.get(encodedPartition);
		if (this.appended.contains(tempFile)) {
			return;
		}
		if (!this.startOffsets.containsKey(encodedPartition)) {
			return;
		}
		final long startOffset = this.startOffsets.get(encodedPartition);
		final long endOffset = this.offsets.get(encodedPartition);
		final String directory = this.getDirectory(encodedPartition);
		final String committedFile = FileUtils.committedFileName(this.url, this.topicsDir, directory, this.tp, startOffset, endOffset, this.extension, this.zeroPadOffsetFormat);
		this.wal.append(tempFile, committedFile);
		this.appended.add(tempFile);
	}

	private void appendToWAL() {
		this.beginAppend();
		for (final String encodedPartition : this.tempFiles.keySet()) {
			this.appendToWAL(encodedPartition);
		}
		this.endAppend();
	}

	private void beginAppend() {
		if (!this.appended.contains("BEGIN")) {
			this.wal.append("BEGIN", "");
		}
	}

	private void endAppend() {
		if (!this.appended.contains("END")) {
			this.wal.append("END", "");
		}
	}

	private void commitFile() {
		TopicPartitionWriter.log.debug("Committing files");
		this.appended.clear();
		for (final String encodedPartition : this.tempFiles.keySet()) {
			this.commitFile(encodedPartition);
		}
	}

	private void commitFile(final String encodedPartition) {
		TopicPartitionWriter.log.debug("Committing file for partition {}", (Object)encodedPartition);
		if (!this.startOffsets.containsKey(encodedPartition)) {
			return;
		}
		final long startOffset = this.startOffsets.get(encodedPartition);
		final long endOffset = this.offsets.get(encodedPartition);
		final String tempFile = this.tempFiles.get(encodedPartition);
		final String directory = this.getDirectory(encodedPartition);
		final String committedFile = FileUtils.committedFileName(this.url, this.topicsDir, directory, this.tp, startOffset, endOffset, this.extension, this.zeroPadOffsetFormat);
		final String directoryName = FileUtils.directoryName(this.url, this.topicsDir, directory);
		if (!this.storage.exists(directoryName)) {
			this.storage.create(directoryName);
		}
		this.storage.commit(tempFile, committedFile);
		this.startOffsets.remove(encodedPartition);
		this.offsets.remove(encodedPartition);
		this.offset += this.recordCounter;
		this.recordCounter = 0;
		TopicPartitionWriter.log.info("Committed {} for {}", (Object)committedFile, (Object)this.tp);
	}

	private void deleteTempFile(final String encodedPartition) {
		this.storage.delete(this.tempFiles.get(encodedPartition));
	}

	private void setRetryTimeout(final long timeoutMs) {
		this.context.timeout(timeoutMs);
	}

	private void createHiveTable() {
		final Future<Void> future = this.executorService.submit((Callable<Void>)new Callable<Void>() {
			@Override
			public Void call() throws HiveMetaStoreException {
				try {
					TopicPartitionWriter.this.hive.createTable(TopicPartitionWriter.this.hiveDatabase, TopicPartitionWriter.this.tp.topic(), TopicPartitionWriter.this.currentSchema, TopicPartitionWriter.this.partitioner);
				}
				catch (Throwable e) {
					TopicPartitionWriter.log.error("Creating Hive table threw unexpected error", e);
				}
				return null;
			}
		});
		this.hiveUpdateFutures.add(future);
	}

	private void alterHiveSchema() {
		final Future<Void> future = this.executorService.submit((Callable<Void>)new Callable<Void>() {
			@Override
			public Void call() throws HiveMetaStoreException {
				try {
					TopicPartitionWriter.this.hive.alterSchema(TopicPartitionWriter.this.hiveDatabase, TopicPartitionWriter.this.tp.topic(), TopicPartitionWriter.this.currentSchema);
				}
				catch (Throwable e) {
					TopicPartitionWriter.log.error("Altering Hive schema threw unexpected error", e);
				}
				return null;
			}
		});
		this.hiveUpdateFutures.add(future);
	}

	private void addHivePartition(final String location) {
		final Future<Void> future = this.executorService.submit((Callable<Void>)new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				try {
					TopicPartitionWriter.this.hiveMetaStore.addPartition(TopicPartitionWriter.this.hiveDatabase, TopicPartitionWriter.this.tp.topic(), location);
				}
				catch (Throwable e) {
					TopicPartitionWriter.log.error("Adding Hive partition threw unexpected error", e);
				}
				return null;
			}
		});
		this.hiveUpdateFutures.add(future);
	}

	static {
		log = LoggerFactory.getLogger((Class)TopicPartitionWriter.class);
		WALLCLOCK = (TimestampExtractor)new TimeBasedPartitioner.WallclockTimestampExtractor();
	}

	private enum State
	{
		RECOVERY_STARTED, 
		RECOVERY_PARTITION_PAUSED, 
		WAL_APPLIED, 
		WAL_TRUNCATED, 
		OFFSET_RESET, 
		WRITE_STARTED, 
		WRITE_PARTITION_PAUSED, 
		SHOULD_ROTATE, 
		TEMP_FILE_CLOSED, 
		WAL_APPENDED, 
		FILE_COMMITTED;

		private static State[] vals;

		public State next() {
			return State.vals[(this.ordinal() + 1) % State.vals.length];
		}

		static {
			State.vals = values();
		}
	}
}
