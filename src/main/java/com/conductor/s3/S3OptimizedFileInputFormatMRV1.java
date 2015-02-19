package com.conductor.s3;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.S3NativeFileSystemConfigKeys;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Lists;

/**
 * A {@link FileInputFormat} (MRV1 API) that is optimized for S3-based input, and supports recursive discovery of input
 * files given a single parent directory.
 * <p>
 * Job start-up time is much faster because this input format uses the {@link com.amazonaws.services.s3.AmazonS3} client
 * to discover job input files rather than the {@link org.apache.hadoop.fs.s3.S3FileSystem}.
 * <p>
 * This {@link FileInputFormat} supports adding just the top-level "directory" (i.e. a single S3 prefix) as file input;
 * it will recursively discover all files under the given prefix. This is <em>much</em> faster than adding individual
 * files to the job.
 * 
 * @author cgreen
 * @see S3SequenceFileInputFormatMRV1
 * @see S3TextFileInputFormatMRV1
 */
public abstract class S3OptimizedFileInputFormatMRV1<K, V> extends FileInputFormat<K, V> {

    @Override
    protected FileStatus[] listStatus(final JobConf job) throws IOException {
        final Path[] dirs = getInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }
        final long blockSize = job.getLong(S3NativeFileSystemConfigKeys.S3_NATIVE_BLOCK_SIZE_KEY,
                S3NativeFileSystemConfigKeys.S3_NATIVE_BLOCK_SIZE_DEFAULT);
        final AmazonS3 s3Client = S3HadoopUtils.getS3Client(job);
        final List<FileStatus> result = S3InputFormatUtils.getFileStatuses(s3Client, blockSize, dirs);
        return result.toArray(new FileStatus[result.size()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputSplit[] getSplits(final JobConf job, final int numSplits) throws IOException {
        long minSize = getMinSplitSize(job);
        long maxSize = getMaxSplitSize(job);
        final List<FileStatus> fileStatuses = Lists.newArrayList(listStatus(job));
        final List<InputSplit> splits = S3InputFormatUtils.convertToInputSplitsMRV1(fileStatuses, minSize, maxSize);

        // Save the number of input files in the job-conf
        job.setLong("mapreduce.input.num.files", fileStatuses.size());

        return splits.toArray(new InputSplit[splits.size()]);
    }

    /**
     * Set the minimum input split size
     *
     * @param job
     *            the job to modify
     * @param size
     *            the minimum size
     */
    public static void setMinInputSplitSize(Job job, long size) {
        job.getConfiguration().setLong("mapred.min.split.size", size);
    }

    /**
     * Get the minimum split size
     *
     * @param job
     *            the job
     * @return the minimum number of bytes that can be in a split
     */
    public static long getMinSplitSize(JobConf job) {
        return job.getLong("mapred.min.split.size", 1L);
    }

    /**
     * Set the maximum split size
     *
     * @param job
     *            the job to modify
     * @param size
     *            the maximum split size
     */
    public static void setMaxInputSplitSize(Job job, long size) {
        job.getConfiguration().setLong("mapred.max.split.size", size);
    }

    /**
     * Get the maximum split size.
     *
     * @param context
     *            the job to look at.
     * @return the maximum number of bytes a split can include
     */
    public static long getMaxSplitSize(JobConf context) {
        return context.getLong("mapred.max.split.size", Long.MAX_VALUE);
    }

}
