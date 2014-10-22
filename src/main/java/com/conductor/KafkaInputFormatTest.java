package com.conductor;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.conductor.kafka.hadoop.*;
import com.conductor.kafka.zk.ZkUtils;

/**
 * @author cgreen
 */
public class KafkaInputFormatTest extends Configured {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("com.conductor.kafka.hadoop.KafkaInputFormat").setLevel(Level.DEBUG);
        final com.conductor.kafka.hadoop.KafkaInputFormat kif = new com.conductor.kafka.hadoop.KafkaInputFormat();
        final Job job = Job.getInstance(new Configuration());
        com.conductor.kafka.hadoop.KafkaInputFormat.setZkConnect(job,
                "zookeeper-01:2181,zookeeper-02:2181,zookeeper-03:2181,zookeeper-04:2181,zookeeper-05:2181");
        com.conductor.kafka.hadoop.KafkaInputFormat.setTopic(job, "collector_all_serp_meta");
        com.conductor.kafka.hadoop.KafkaInputFormat.setConsumerGroup(job, "casey_green_testing");
        com.conductor.kafka.hadoop.KafkaInputFormat.setMaxSplitsPerPartition(job, Integer.parseInt(args[1]));
        com.conductor.kafka.hadoop.KafkaInputFormat.setIncludeOffsetsAfterTimestamp(job, Long.parseLong(args[0]));

        final List<InputSplit> splits = kif.getSplits(job);

        System.out.println(splits);

        // new KafkaInputFormatTest().runJob();
        // new KafkaInputFormatTest().runJobWithMultipleQueues();
    }

    public void runJob() throws Exception {

        // Create a new job
        final Job job = Job.getInstance(getConf(), "my_job");

        // Set your Zookeeper connection string
        KafkaInputFormat.setZkConnect(job, "zookeeper-1.xyz.com:2181");

        // Set the topic you want to consume
        KafkaInputFormat.setTopic(job, "my_topic");

        // Set the consumer group associated with this job
        KafkaInputFormat.setConsumerGroup(job, "my_consumer_group");

        // Set the mapper that will consume the data
        job.setMapperClass(MyMapper.class);

        // Only commit offsets if the job is successful
        if (job.waitForCompletion(true)) {
            final ZkUtils zk = new ZkUtils(job.getConfiguration());
            zk.commit("my_consumer_group", "my_topic");
            zk.close();
        }
    }

    public void runJobWithMultipleQueues() throws Exception {
        // Create a new job
        final Job job = Job.getInstance(getConf(), "my_job");

        // Set your Zookeeper connection string
        KafkaInputFormat.setZkConnect(job, "zookeeper-1.xyz.com:2181");

        // Add as many queue inputs as you'd like
        MultipleKafkaInputFormat.addTopic(job, "my_first_topic", "my_consumer_group", MyMapper.class);
        MultipleKafkaInputFormat.addTopic(job, "my_second_topic", "my_consumer_group", MyMapper.class);
        // ...

        // Only commit offsets if the job is successful
        if (job.waitForCompletion(true)) {
            final ZkUtils zk = new ZkUtils(job.getConfiguration());
            // commit the offsets for each topic
            zk.commit("my_consumer_group", "my_first_topic");
            zk.commit("my_consumer_group", "my_second_topic");
            // ...
            zk.close();
        }
    }

    public void blah() throws Exception {

        final KafkaJobBuilder jobBuilder = KafkaJobBuilder.newBuilder()
        // Zookeeper connection string
                .setZkConnect("zookeeper-1.xyz.com:2181")
                // Add any number of queue inputs
                .addQueueInput("my_first_topic", "my_consumer_group", MyMapper.class)
                // Specify a Null/Sequence/TextOutputFormat
                .setSequenceFileOutputFormat("/my/hdfs/path");

        final Job job = jobBuilder.configureJob(getConf());

        // Only commit offsets if the job is successful
        if (job.waitForCompletion(true)) {
            final ZkUtils zk = new ZkUtils(job.getConfiguration());
            zk.commit("my_consumer_group", "my_topic");
            zk.close();
        }

    }

    public static class MyMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {
    }
}
