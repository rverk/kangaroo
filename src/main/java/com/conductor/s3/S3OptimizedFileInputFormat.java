package com.conductor.s3;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.S3NativeFileSystemConfigKeys;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.amazonaws.services.s3.AmazonS3;

/**
 * A {@link FileInputFormat} (MRV2 API) that is optimized for S3-based input, and supports recursive discovery of input
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
 * @see S3SequenceFileInputFormat
 * @see S3TextInputFormat
 * @author cgreen
 */
public abstract class S3OptimizedFileInputFormat<K, V> extends FileInputFormat<K, V> {

    @Override
    protected List<FileStatus> listStatus(final JobContext job) throws IOException {
        final Path[] dirs = getInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }
        final long blockSize = job.getConfiguration().getLong(S3NativeFileSystemConfigKeys.S3_NATIVE_BLOCK_SIZE_KEY,
                S3NativeFileSystemConfigKeys.S3_NATIVE_BLOCK_SIZE_DEFAULT);
        final AmazonS3 s3Client = S3HadoopUtils.getS3Client(job.getConfiguration());
        return S3InputFormatUtils.getFileStatuses(s3Client, blockSize, dirs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<InputSplit> getSplits(final JobContext job) throws IOException {
        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);
        final List<FileStatus> files = listStatus(job);
        return S3InputFormatUtils.convertToInputSplits(files, minSize, maxSize);
    }

}
