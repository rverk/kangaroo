package com.conductor.s3;

import java.io.IOException;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

/**
 * Copied directly from {@link org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat} (except the wrapper around
 * {@link #listStatus} which is not needed in this case), but inherits from the S3 optimized input format
 * {@link S3OptimizedFileInputFormat}.
 *
 * @author cgreen
 * @see S3OptimizedFileInputFormatMRV1
 */
public class S3SequenceFileInputFormat<K, V> extends S3OptimizedFileInputFormat<K, V> {

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new SequenceFileRecordReader<K, V>();
    }

    @Override
    protected long getFormatMinSplitSize() {
        return SequenceFile.SYNC_INTERVAL;
    }
}
