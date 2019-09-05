// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.tools;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import java.util.List;
import java.util.Collections;
import org.apache.kafka.connect.errors.ConnectException;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.apache.kafka.connect.source.SourceTask;

public class SchemaSourceTask extends SourceTask
{
    public static final String NAME_CONFIG = "name";
    public static final String ID_CONFIG = "id";
    public static final String TOPIC_CONFIG = "topic";
    public static final String NUM_MSGS_CONFIG = "num.messages";
    public static final String THROUGHPUT_CONFIG = "throughput";
    public static final String MULTIPLE_SCHEMA_CONFIG = "multiple.schema";
    public static final String PARTITION_COUNT_CONFIG = "partition.count";
    private static final Logger log;
    private static final String ID_FIELD = "id";
    private static final String SEQNO_FIELD = "seqno";
    private static Schema valueSchema;
    private static Schema valueSchema2;
    private String name;
    private int id;
    private String topic;
    private Map<String, Integer> partition;
    private long startingSeqno;
    private long seqno;
    private long count;
    private long maxNumMsgs;
    private boolean multipleSchema;
    private int partitionCount;
    private long intervalMs;
    private int intervalNanos;
    
    public String version() {
        return new SchemaSourceConnector().version();
    }
    
    public void start(final Map<String, String> props) {
        try {
            this.name = props.get("name");
            this.id = Integer.parseInt(props.get("id"));
            this.topic = props.get("topic");
            this.maxNumMsgs = Long.parseLong(props.get("num.messages"));
            this.multipleSchema = Boolean.parseBoolean(props.get("multiple.schema"));
            this.partitionCount = Integer.parseInt(props.containsKey("partition.count") ? props.get("partition.count") : "1");
            final String throughputStr = props.get("throughput");
            if (throughputStr != null) {
                final long throughput = Long.parseLong(throughputStr);
                final long intervalTotalNanos = 1000000000L / throughput;
                this.intervalMs = intervalTotalNanos / 1000000L;
                this.intervalNanos = (int)(intervalTotalNanos % 1000000L);
            }
            else {
                this.intervalMs = 0L;
                this.intervalNanos = 0;
            }
        }
        catch (NumberFormatException e) {
            throw new ConnectException("Invalid SchemaSourceTask configuration", (Throwable)e);
        }
        partition = Collections.singletonMap(ID_FIELD, this.id);
        Map<String, Object> previousOffset = context.offsetStorageReader().offset(partition);
        if (previousOffset != null) {
            this.seqno = (Long) previousOffset.get(SEQNO_FIELD) + 1L;
        }
        else {
            this.seqno = 0L;
        }
        this.startingSeqno = this.seqno;
        this.count = 0L;
        SchemaSourceTask.log.info("Started SchemaSourceTask {}-{} producing to topic {} resuming from seqno {}", new Object[] { this.name, this.id, this.topic, this.startingSeqno });
    }
    
    public List<SourceRecord> poll() throws InterruptedException {
        if (this.count < this.maxNumMsgs) {
            if (this.intervalMs > 0L || this.intervalNanos > 0) {
                synchronized (this) {
                    this.wait(this.intervalMs, this.intervalNanos);
                }
            }
            final Map<String, Long> ccOffset = Collections.singletonMap("seqno", this.seqno);
            final int partitionVal = (int)(this.seqno % this.partitionCount);
            SourceRecord srcRecord;
            if (!this.multipleSchema || this.count % 2L == 0L) {
                Struct data = new Struct(SchemaSourceTask.valueSchema).put("boolean", (Object)true).put("int", (Object)12).put("long", (Object)12L).put("float", (Object)12.2f).put("double", (Object)12.2).put("partitioning", (Object)partitionVal).put("id", (Object)this.id).put("seqno", (Object)this.seqno);
                srcRecord = new SourceRecord(partition, ccOffset, topic, Integer.valueOf(this.id), Schema.STRING_SCHEMA, "key", SchemaSourceTask.valueSchema, data);
            }
            else {
                Struct data = new Struct(SchemaSourceTask.valueSchema2).put("boolean", (Object)true).put("int", (Object)12).put("long", (Object)12L).put("float", (Object)12.2f).put("double", (Object)12.2).put("partitioning", (Object)partitionVal).put("string", (Object)"def").put("id", (Object)this.id).put("seqno", (Object)this.seqno);
                srcRecord = new SourceRecord(this.partition, ccOffset, topic, Integer.valueOf(this.id), Schema.STRING_SCHEMA, "key", SchemaSourceTask.valueSchema2, data);
            }
            System.out.println("{\"task\": " + this.id + ", \"seqno\": " + this.seqno + "}");
            final List<SourceRecord> result = Arrays.asList(srcRecord);
            ++this.seqno;
            ++this.count;
            return result;
        }
        synchronized (this) {
            this.wait();
        }
        return new ArrayList<SourceRecord>();
    }
    
    public void stop() {
        synchronized (this) {
            this.notifyAll();
        }
    }
    
    static {
        log = LoggerFactory.getLogger(SchemaSourceTask.class);
        SchemaSourceTask.valueSchema = SchemaBuilder.struct().version(Integer.valueOf(1)).name("record").field("boolean", Schema.BOOLEAN_SCHEMA).field("int", Schema.INT32_SCHEMA).field("long", Schema.INT64_SCHEMA).field("float", Schema.FLOAT32_SCHEMA).field("double", Schema.FLOAT64_SCHEMA).field("partitioning", Schema.INT32_SCHEMA).field("id", Schema.INT32_SCHEMA).field("seqno", Schema.INT64_SCHEMA).build();
        SchemaSourceTask.valueSchema2 = SchemaBuilder.struct().version(Integer.valueOf(2)).name("record").field("boolean", Schema.BOOLEAN_SCHEMA).field("int", Schema.INT32_SCHEMA).field("long", Schema.INT64_SCHEMA).field("float", Schema.FLOAT32_SCHEMA).field("double", Schema.FLOAT64_SCHEMA).field("partitioning", Schema.INT32_SCHEMA).field("string", SchemaBuilder.string().defaultValue((Object)"abc").build()).field("id", Schema.INT32_SCHEMA).field("seqno", Schema.INT64_SCHEMA).build();
    }
}
