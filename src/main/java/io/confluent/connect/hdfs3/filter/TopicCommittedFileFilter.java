// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.filter;

import java.util.regex.Matcher;
import io.confluent.connect.hdfs3.Constants;
import org.apache.hadoop.fs.Path;

public class TopicCommittedFileFilter extends CommittedFileFilter
{
    private String topic;
    
    public TopicCommittedFileFilter(final String topic) {
        this.topic = topic;
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
        return topic.equals(this.topic);
    }
}
