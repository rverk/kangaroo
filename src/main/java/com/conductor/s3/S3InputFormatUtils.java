package com.conductor.s3;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Package-private utility class containing all of the shared code between {@link S3OptimizedFileInputFormatMRV1} and
 * {@link S3OptimizedFileInputFormat}.
 * 
 * @author cgreen
 */
final class S3InputFormatUtils {

    private S3InputFormatUtils() {
    }

    /* 10% slop, taken directly from file input formats */
    private static final double SPLIT_SLOP = 1.1;

    /* For S3 file splits, this is what hostname is set to */
    private static final String[] S3_SPLIT_HOST = new String[] { "localhost" };

    /**
     * A {@link org.apache.hadoop.fs.PathFilter} that filters out hidden files, job log files, and Hive $folder$ files.
     */
    private static final PathFilter S3_PATH_FILTER = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".") && !name.endsWith("$folder$");
        }
    };

    /**
     * Converts from MRV1 API to MRV2 API File input splits.
     */
    @VisibleForTesting
    static final Function<InputSplit, org.apache.hadoop.mapreduce.InputSplit> SPLIT_CONVERTER = new Function<InputSplit, org.apache.hadoop.mapreduce.InputSplit>() {
        @Override
        public org.apache.hadoop.mapreduce.InputSplit apply(final InputSplit input) {
            final FileSplit split = (FileSplit) input;
            return new org.apache.hadoop.mapreduce.lib.input.FileSplit(split.getPath(), split.getStart(),
                    split.getLength(), S3_SPLIT_HOST);
        }
    };

    /**
     * Efficiently gets the Hadoop {@link org.apache.hadoop.fs.FileStatus} for all S3 files under the provided
     * {@code dirs}
     * 
     * @param s3Client
     *            s3 client
     * @param blockSize
     *            the block size
     * @param dirs
     *            the dirs to search through
     * @return the {@link org.apache.hadoop.fs.FileStatus} version of all S3 files under {@code dirs}
     */
    static List<FileStatus> getFileStatuses(final AmazonS3 s3Client, final long blockSize, final Path... dirs) {
        final List<FileStatus> result = Lists.newArrayList();
        for (final Path dir : dirs) {
            // get bucket and prefix from path
            final String bucket = S3HadoopUtils.getBucketFromPath(dir.toString());
            final String prefix = S3HadoopUtils.getKeyFromPath(dir.toString());
            // list request
            final ListObjectsRequest req = new ListObjectsRequest().withMaxKeys(Integer.MAX_VALUE)
                    .withBucketName(bucket).withPrefix(prefix);
            // recursively page through all objects under the path
            for (ObjectListing listing = s3Client.listObjects(req); listing.getObjectSummaries().size() > 0; listing = s3Client
                    .listNextBatchOfObjects(listing)) {
                for (final S3ObjectSummary summary : listing.getObjectSummaries()) {
                    final Path path = new Path(String.format("s3n://%s/%s", summary.getBucketName(), summary.getKey()));
                    if (S3_PATH_FILTER.accept(path)) {
                        result.add(new FileStatus(summary.getSize(), false, 1, blockSize, summary.getLastModified()
                                .getTime(), path));
                    }
                }
                // don't need to check the next listing if this one is not truncated
                if (!listing.isTruncated()) {
                    break;
                }
            }
        }
        return result;
    }

    /**
     * Converts the {@link org.apache.hadoop.fs.FileStatus}s to {@link org.apache.hadoop.mapred.InputSplit}s (MRV1 API).
     * <p>
     * This is taken directly from {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}, less any file system
     * operations that do not make sense when using {@code S3}.
     * 
     * @param files
     *            the files to convert
     * @param minSize
     *            the minimum size of the splits
     * @param maxSize
     *            the maximum size of the splits
     * @return the splits.
     */
    static List<InputSplit> convertToInputSplitsMRV1(final Iterable<FileStatus> files, final long minSize,
            final long maxSize) {
        final List<InputSplit> splits = Lists.newArrayList();
        for (final FileStatus file : files) {
            // check for valid data for this input format
            checkArgument(!file.isDirectory(), "Cannot pass directories to this method!");
            final String path = file.getPath().toString();
            checkArgument(path.startsWith("s3:") || path.startsWith("s3n:"), "Expected S3 input");

            // create splits out of file
            final long length = file.getLen();
            if (length > 0) {
                long blockSize = file.getBlockSize();
                long splitSize = computeSplitSize(blockSize, minSize, maxSize);
                long bytesRemaining = length;
                while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
                    splits.add(new FileSplit(file.getPath(), length - bytesRemaining, splitSize, S3_SPLIT_HOST));
                    bytesRemaining -= splitSize;
                }
                if (bytesRemaining != 0) {
                    splits.add(new FileSplit(file.getPath(), length - bytesRemaining, bytesRemaining, S3_SPLIT_HOST));
                }
            }
        }
        return splits;
    }

    /**
     * Converts the {@link org.apache.hadoop.fs.FileStatus}s to {@link org.apache.hadoop.mapreduce.InputSplit}s.
     * 
     * @param files
     *            the files to convert
     * @param minSize
     *            the minimum size of the splits
     * @param maxSize
     *            the maximum size of the splits
     * @return the splits for the files.
     */
    static List<org.apache.hadoop.mapreduce.InputSplit> convertToInputSplits(final Iterable<FileStatus> files,
            final long minSize, final long maxSize) {
        return Lists.transform(convertToInputSplitsMRV1(files, minSize, maxSize), SPLIT_CONVERTER);
    }

    /**
     * This is exactly the same as {@link org.apache.hadoop.mapred.FileInputFormat#computeSplitSize} and
     * {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat#computeSplitSize}
     */
    private static long computeSplitSize(long blockSize, long minSize, long maxSize) {
        return Math.max(minSize, Math.min(maxSize, blockSize));
    }
}
