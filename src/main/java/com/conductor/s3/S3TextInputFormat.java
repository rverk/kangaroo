package com.conductor.s3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Copied directly from {@link org.apache.hadoop.mapreduce.lib.input.TextInputFormat}, but inherits from the S3
 * optimized input format {@link S3OptimizedFileInputFormatMRV1}.
 *
 * @author cgreen
 * @see S3OptimizedFileInputFormatMRV1
 */
public class S3TextInputFormat extends S3OptimizedFileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        final String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
        return new LineRecordReader(delimiter != null ? delimiter.getBytes() : null);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return null == codec || codec instanceof SplittableCompressionCodec;
    }
}
