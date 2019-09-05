// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3;

import java.util.regex.Pattern;

public class Constants
{
    public static final String COMMMITTED_FILENAME_SEPARATOR = "+";
    public static final Pattern COMMITTED_FILENAME_PATTERN;
    public static final int PATTERN_TOPIC_GROUP = 1;
    public static final int PATTERN_PARTITION_GROUP = 2;
    public static final int PATTERN_START_OFFSET_GROUP = 3;
    public static final int PATTERN_END_OFFSET_GROUP = 4;
    public static final String TEMPFILE_DIRECTORY = "/+tmp/";
    
    static {
        COMMITTED_FILENAME_PATTERN = Pattern.compile("([a-zA-Z0-9\\._\\-]+)\\+(\\d+)\\+(\\d+)\\+(\\d+)(.\\w+)?");
    }
}
