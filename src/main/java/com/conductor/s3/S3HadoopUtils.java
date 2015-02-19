package com.conductor.s3;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.base.Strings;

/**
 * Some useful S3 Hadoop utilities shared by all of the input formats.
 *
 * @author cgreen
 */
public final class S3HadoopUtils {

    private S3HadoopUtils() {
    }

    /**
     * Extracts the AWS key and secret from the conf, and returns a new S3 client.
     * 
     * @param conf
     *            job conf
     * @return an S3 client.
     * @throws java.lang.IllegalArgumentException
     *             if it cannot find key/id in the conf.
     */
    public static AmazonS3 getS3Client(final Configuration conf) {
        final String accessKey = conf.get("fs.s3n.awsAccessKeyId", conf.get("fs.s3.awsAccessKeyId"));
        checkArgument(!Strings.isNullOrEmpty(accessKey), "Missing fs.s3/n.awsAccessKeyId conf.");
        final String secretKey = conf.get("fs.s3n.awsSecretAccessKey", conf.get("fs.s3.awsSecretAccessKey"));
        checkArgument(!Strings.isNullOrEmpty(secretKey), "Missing fs.s3/n.awsSecretAccessKey conf.");
        return new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    }

    /**
     * Extracts the S3 bucket from the S3 path.
     * <p/>
     * An "S3 path" is of the form {@code s3://bucket-name/path/or/path/prefix} or
     * {@code s3n://bucket-name/path/or/path/prefix}. In either of these examples, this function would return
     * {@code bucket-name}.
     *
     * @param path
     *            the S3 path.
     * @return the S3 bucket, or null if {@code path} is not an S3 path.
     */
    public static String getBucketFromPath(final String path) {
        if (!path.startsWith("s3:") && !path.startsWith("s3n:")) {
            return null;
        }
        return path.replaceFirst("(s3://|s3n://)", "").split("/")[0];
    }

    /**
     * Extracts the key (or key prefix depending on {@code path}) from the S3 path.
     * <p/>
     * An "S3 path" is of the form {@code s3://bucket-name/path/or/path/prefix} or
     * {@code s3n://bucket-name/path/or/path/prefix}. In either of these examples, this function would return
     * {@code path/or/path/prefix}.
     *
     * @param path
     *            the S3 path.
     * @return the S3 key portion of the path (or key prefix depending on {@code path}), or null if {@code path} is not
     *         an S3 path.
     */
    public static String getKeyFromPath(final String path) {
        if (Strings.isNullOrEmpty(path) || !path.startsWith("s3:") && !path.startsWith("s3n:")) {
            return null;
        }
        final String res = path.replaceFirst("(s3://|s3n://)", "");
        final int i = res.indexOf('/');
        return i != -1 ? res.substring(i + 1) : null;
    }

}
