// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3.filter;

import java.util.regex.Matcher;
import io.confluent.connect.hdfs3.Constants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class CommittedFileFilter implements PathFilter
{
	@Override
    public boolean accept(final Path path) {
        final String filename = path.getName();
        final Matcher m = Constants.COMMITTED_FILENAME_PATTERN.matcher(filename);
        return m.matches();
    }
}
