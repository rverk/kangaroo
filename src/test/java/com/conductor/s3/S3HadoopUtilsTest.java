package com.conductor.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class S3HadoopUtilsTest {

    @Test
    public void testGetBucketFromPath() throws Exception {
        assertEquals("bucket-name", S3HadoopUtils.getBucketFromPath("s3n://bucket-name/a/very/long/path/to/something"));
        assertEquals("bucket-name", S3HadoopUtils.getBucketFromPath("s3://bucket-name/a/very/long/path/to/something"));

        assertNull(S3HadoopUtils.getBucketFromPath("hdfs://a/very/long/path/to/something"));
    }

    @Test
    public void testGetPathPrefixFromPath() throws Exception {
        assertEquals("a/very/long/path/to/something",
                S3HadoopUtils.getKeyFromPath("s3n://bucket-name/a/very/long/path/to/something"));
        assertEquals("a/very/long/path/to/something",
                S3HadoopUtils.getKeyFromPath("s3://bucket-name/a/very/long/path/to/something"));
        // trailing slash
        assertEquals("a/very/long/path/to/something/",
                S3HadoopUtils.getKeyFromPath("s3://bucket-name/a/very/long/path/to/something/"));

        assertNull(S3HadoopUtils.getKeyFromPath("hdfs://a/very/long/path/to/something"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetS3ClientNoKey() throws Exception {
        S3HadoopUtils.getS3Client(new Configuration(false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetS3ClientNoSecrety() throws Exception {
        final Configuration conf = new Configuration(false);
        conf.set("fs.s3.awsAccessKeyId", "WDFIUB435DF834");
        S3HadoopUtils.getS3Client(conf);
    }
}
