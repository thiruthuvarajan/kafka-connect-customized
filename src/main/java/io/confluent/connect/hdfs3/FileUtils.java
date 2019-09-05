// 
// Decompiled by Procyon v0.5.36
// 

package io.confluent.connect.hdfs3;

import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import io.confluent.connect.hdfs3.filter.CommittedFileFilter;
import org.apache.hadoop.fs.Path;
import io.confluent.connect.hdfs3.storage.HdfsStorage;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

public class FileUtils
{
    private static final Logger log;
    
    public static String logFileName(final String url, final String logsDir, final TopicPartition topicPart) {
        return fileName(url, logsDir, topicPart, "log");
    }
    
    public static String directoryName(final String url, final String topicsDir, final TopicPartition topicPart) {
        final String topic = topicPart.topic();
        final int partition = topicPart.partition();
        return url + "/" + topicsDir + "/" + topic + "/" + partition;
    }
    
    public static String directoryName(final String url, final String topicsDir, final String directory) {
        return url + "/" + topicsDir + "/" + directory;
    }
    
    public static String fileName(final String url, final String topicsDir, final TopicPartition topicPart, final String name) {
        final String topic = topicPart.topic();
        final int partition = topicPart.partition();
        return url + "/" + topicsDir + "/" + topic + "/" + partition + "/" + name;
    }
    
    public static String fileName(final String url, final String topicsDir, final String directory, final String name) {
        return url + "/" + topicsDir + "/" + directory + "/" + name;
    }
    
    public static String tempFileName(final String url, final String topicsDir, final String directory, final String extension) {
        final UUID id = UUID.randomUUID();
        final String name = id.toString() + "_tmp" + extension;
        return fileName(url, topicsDir, directory, name);
    }
    
    public static String committedFileName(final String url, final String topicsDir, final String directory, final TopicPartition topicPart, final long startOffset, final long endOffset, final String extension, final String zeroPadFormat) {
        final String topic = topicPart.topic();
        final int partition = topicPart.partition();
        final StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(Constants.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(partition);
        sb.append(Constants.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(String.format(zeroPadFormat, startOffset));
        sb.append(Constants.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(String.format(zeroPadFormat, endOffset));
        sb.append(extension);
        final String name = sb.toString();
        return fileName(url, topicsDir, directory, name);
    }
    
    public static String topicDirectory(final String url, final String topicsDir, final String topic) {
        return url + "/" + topicsDir + "/" + topic;
    }
    
    public static FileStatus fileStatusWithMaxOffset(final HdfsStorage storage, final Path path, final CommittedFileFilter filter) {
        if (!storage.exists(path.toString())) {
            return null;
        }
        long maxOffset = -1L;
        FileStatus fileStatusWithMaxOffset = null;
        final List<FileStatus> statuses = storage.list(path.toString());
        for (final FileStatus status : statuses) {
            if (status.isDirectory()) {
                final FileStatus fileStatus = fileStatusWithMaxOffset(storage, status.getPath(), filter);
                if (fileStatus == null) {
                    continue;
                }
                final long offset = extractOffset(fileStatus.getPath().getName());
                if (offset <= maxOffset) {
                    continue;
                }
                maxOffset = offset;
                fileStatusWithMaxOffset = fileStatus;
            }
            else {
                final String filename = status.getPath().getName();
                FileUtils.log.trace("Checked for max offset: {}", (Object)status.getPath());
                if (!filter.accept(status.getPath())) {
                    continue;
                }
                final long offset = extractOffset(filename);
                if (offset <= maxOffset) {
                    continue;
                }
                maxOffset = offset;
                fileStatusWithMaxOffset = status;
            }
        }
        return fileStatusWithMaxOffset;
    }
    
    public static long extractOffset(final String filename) {
        final Matcher m = Constants.COMMITTED_FILENAME_PATTERN.matcher(filename);
        if (!m.matches()) {
            throw new IllegalArgumentException(filename + " does not match COMMITTED_FILENAME_PATTERN");
        }
        return Long.parseLong(m.group(4));
    }
    
    private static ArrayList<FileStatus> getDirectoriesImpl(final HdfsStorage storage, final Path path) {
        final List<FileStatus> statuses = storage.list(path.toString());
        final ArrayList<FileStatus> result = new ArrayList<FileStatus>();
        for (final FileStatus status : statuses) {
            if (status.isDirectory()) {
                int count = 0;
                final List<FileStatus> fileStatuses = storage.list(status.getPath().toString());
                for (final FileStatus fileStatus : fileStatuses) {
                    if (fileStatus.isDirectory()) {
                        result.addAll(getDirectoriesImpl(storage, fileStatus.getPath()));
                    }
                    else {
                        ++count;
                    }
                }
                if (count != fileStatuses.size()) {
                    continue;
                }
                result.add(status);
            }
        }
        return result;
    }
    
    public static FileStatus[] getDirectories(final HdfsStorage storage, final Path path) throws IOException {
        final ArrayList<FileStatus> result = getDirectoriesImpl(storage, path);
        return result.toArray(new FileStatus[result.size()]);
    }
    
    private static ArrayList<FileStatus> traverseImpl(final HdfsStorage storage, final Path path, final PathFilter filter) {
        if (!storage.exists(path.toString())) {
            return new ArrayList<FileStatus>();
        }
        final ArrayList<FileStatus> result = new ArrayList<FileStatus>();
        final List<FileStatus> statuses = storage.list(path.toString());
        for (final FileStatus status : statuses) {
            if (status.isDirectory()) {
                result.addAll(traverseImpl(storage, status.getPath(), filter));
            }
            else {
                if (!filter.accept(status.getPath())) {
                    continue;
                }
                result.add(status);
            }
        }
        return result;
    }
    
    private static ArrayList<FileStatus> traverseImpl(final FileSystem fs, final Path path) throws IOException {
        if (!fs.exists(path)) {
            return new ArrayList<FileStatus>();
        }
        final ArrayList<FileStatus> result = new ArrayList<FileStatus>();
        final FileStatus[] listStatus;
        final FileStatus[] statuses = listStatus = fs.listStatus(path);
        for (final FileStatus status : listStatus) {
            if (status.isDirectory()) {
                result.addAll(traverseImpl(fs, status.getPath()));
            }
            else {
                result.add(status);
            }
        }
        return result;
    }
    
    public static FileStatus[] traverse(final HdfsStorage storage, final Path path, final PathFilter filter) throws IOException {
        final ArrayList<FileStatus> result = traverseImpl(storage, path, filter);
        return result.toArray(new FileStatus[result.size()]);
    }
    
    public static FileStatus[] traverse(final FileSystem fs, final Path path) throws IOException {
        final ArrayList<FileStatus> result = traverseImpl(fs, path);
        return result.toArray(new FileStatus[result.size()]);
    }
    
    static {
        log = LoggerFactory.getLogger(FileUtils.class);
    }
}
