package com.conductor.kafka.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.junit.Test;

import com.conductor.hadoop.DelegatingMapper;

/**
 * @author cgreen
 */
public class KafkaJobBuilderTest {

    private Configuration conf = new Configuration(false);
    final KafkaJobBuilder builder = KafkaJobBuilder.newBuilder();

    @Test
    public void testBaseConfigure() throws Exception {
        builder.setZkConnect("localhost:2181");
        builder.addQueueInput("queue_name", "group_name", MockMapper.class);
        builder.setNullOutputFormat();
        builder.configureJob(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRequiredZk() throws Exception {
        // set other required settings
        builder.addQueueInput("queue_name", "group_name", MockMapper.class);
        builder.setNullOutputFormat();
        builder.configureJob(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRequiredQueues() throws Exception {
        // set other required settings
        builder.setZkConnect("localhost:2181");
        builder.setNullOutputFormat();
        builder.configureJob(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRequiredOutput() throws Exception {
        // set other required settings
        builder.setZkConnect("localhost:2181");
        builder.addQueueInput("queue_name", "group_name", MockMapper.class);
        builder.configureJob(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testS3MissingDefaultBucket() throws Exception {
        builder.setZkConnect("localhost:2181");
        builder.addQueueInput("queue_name", "group_name", MockMapper.class);
        builder.useS3("key", "seceret", null);
        builder.setTextFileOutputFormat();
        builder.configureJob(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testS3MissingCredentials() throws Exception {
        builder.setZkConnect("localhost:2181");
        builder.addQueueInput("queue_name", "group_name", MockMapper.class);
        builder.setTextFileOutputFormat("s3://bucket/path");
        builder.configureJob(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testS3NMissingCredentials() throws Exception {
        builder.setZkConnect("localhost:2181");
        builder.addQueueInput("queue_name", "group_name", MockMapper.class);
        builder.setTextFileOutputFormat("s3n://bucket/path");
        builder.configureJob(conf);
    }

    @Test
    public void testConfigureWholeJob() throws Exception {
        // base configuration
        builder.setZkConnect("localhost:2181");
        builder.addQueueInput("queue_name", "group_name", MockMapper.class);
        builder.setTextFileOutputFormat("/a/hdfs/path");

        // extended configuration
        builder.setJobName("job_name");
        builder.setMapOutputKeyClass(Text.class);
        builder.setMapOutputValueClass(BytesWritable.class);
        builder.setReducerClass(MockReducer.class);
        builder.setTaskMemorySettings("-Xmx2048m");
        builder.setNumReduceTasks(100);
        builder.setParitioner(MockPartitioner.class);
        builder.setKafkaFetchSizeBytes(1024);

        Job job = builder.configureJob(conf);

        assertEquals("job_name", job.getJobName());
        assertEquals(Text.class, job.getMapOutputKeyClass());
        assertEquals(BytesWritable.class, job.getMapOutputValueClass());
        assertEquals(MockReducer.class, job.getReducerClass());
        assertEquals(MockMapper.class, job.getMapperClass());
        assertEquals("-Xmx2048m", job.getConfiguration().get("mapred.child.java.opts"));
        assertEquals(100, job.getNumReduceTasks());
        assertEquals(MockPartitioner.class, job.getPartitionerClass());
        assertEquals(1024, KafkaInputFormat.getKafkaFetchSizeBytes(job.getConfiguration()));
        assertEquals(TextOutputFormat.class, job.getOutputFormatClass());
        assertEquals(KafkaInputFormat.class, job.getInputFormatClass());
        assertEquals("file:/a/hdfs/path", TextOutputFormat.getOutputPath(job).toString());

        builder.setJobName(null);
        builder.setSequenceFileOutputFormat();
        builder.setUseLazyOutput();
        builder.addQueueInput("queue_name_2", "group_name_2", MockMapper.class);

        job = builder.configureJob(conf);
        assertEquals(LazyOutputFormat.class, job.getOutputFormatClass());
        assertEquals(MultipleKafkaInputFormat.class, job.getInputFormatClass());
        assertEquals(DelegatingMapper.class, job.getMapperClass());
        assertEquals(BytesWritable.class, job.getOutputKeyClass());
        assertEquals(BytesWritable.class, job.getOutputValueClass());
        assertNotNull(SequenceFileOutputFormat.getOutputPath(job));
        assertNotNull(job.getJobName());
    }

    private static class MockMapper extends Mapper {
    }

    private static class MockReducer extends Reducer {
    }

    private static class MockPartitioner extends Partitioner {
        @Override
        public int getPartition(final Object o, final Object o2, final int numPartitions) {
            return 0;
        }
    }
}
