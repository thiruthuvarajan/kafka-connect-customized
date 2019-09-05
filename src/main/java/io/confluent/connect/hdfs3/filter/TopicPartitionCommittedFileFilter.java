// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.filter;

import java.util.regex.Matcher;
import io.confluent.connect.hdfs3.Constants;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;

public class TopicPartitionCommittedFileFilter extends CommittedFileFilter
{
    private TopicPartition tp;
    
    public TopicPartitionCommittedFileFilter(final TopicPartition tp) {
        this.tp = tp;
    }
    
    @Override
    public boolean accept(final Path path) {
        if (!super.accept(path)) {
            return false;
        }
        final String filename = path.getName();
        final Matcher m = Constants.COMMITTED_FILENAME_PATTERN.matcher(filename);
        if (!m.matches()) {
            throw new AssertionError((Object)"match expected because of CommittedFileFilter");
        }
        final String topic = m.group(1);
        final int partition = Integer.parseInt(m.group(2));
        return topic.equals(this.tp.topic()) && partition == this.tp.partition();
    }
}
