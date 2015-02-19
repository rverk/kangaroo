package com.conductor.s3;

import java.io.IOException;

import org.apache.hadoop.mapred.*;

/**
 * Copied directly from {@link org.apache.hadoop.mapred.SequenceFileInputFormat}, but inherits from the S3 optimized
 * input format {@link S3OptimizedFileInputFormatMRV1}.
 *
 * @author cgreen
 * @see S3OptimizedFileInputFormatMRV1
 */
public class S3SequenceFileInputFormatMRV1<K, V> extends S3OptimizedFileInputFormatMRV1<K, V> {

    @Override
    public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(split.toString());
        return new SequenceFileRecordReader<K, V>(job, (FileSplit) split);
    }
}