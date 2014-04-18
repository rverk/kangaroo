package com.conductor.kafka.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import kafka.api.OffsetRequest;
import kafka.consumer.SimpleConsumer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.conductor.kafka.Broker;
import com.conductor.kafka.Partition;
import com.conductor.zk.ZkUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * An {@link InputFormat} that splits up Kafka {@link Broker}-{@link Partition}s further into a set of offsets.
 * 
 * <p/>
 * Specifically, it will call {@link SimpleConsumer#getOffsetsBefore(String, int, long, int)} to retrieve a list of
 * valid offsets, and create {@code N} number of {@link InputSplit}s per {@link Broker}-{@link Partition}, where
 * {@code N} is the number of offsets returned by {@link SimpleConsumer#getOffsetsBefore(String, int, long, int)}.
 * 
 * <p/>
 * Thank you <a href="https://github.com/miniway">Dongmin Yu</a> for the inspiration of this class.
 * 
 * <p/>
 * The original source code can be found <a target="_blank" href="https://github.com/miniway/kafka-hadoop-consumer">on
 * Github</a>.
 * 
 * @see KafkaInputSplit
 * @see KafkaRecordReader
 * 
 * @author <a href="mailto:miniway@gmail.com">Dongmin Yu</a>
 * @author <a href="mailto:cgreen@conductor.com">Casey Green</a>
 */
public class KafkaInputFormat extends InputFormat<LongWritable, BytesWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaInputFormat.class);

    /**
     * Default Kafka fetch size, 1MB.
     */
    public static final int DEFAULT_FETCH_SIZE = 1024 * 1024; // 1MB
    /**
     * Default Kafka socket timeout, 10 seconds.
     */
    public static final int DEFAULT_SOCKET_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(10);
    /**
     * Default Kafka buffer size, 64KB.
     */
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024; // 64 KB
    /**
     * Default Zookeeper session timeout, 10 seconds.
     */
    public static final int DEFAULT_ZK_SESSION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(10);
    /**
     * Default Zookeeper connection timeout, 10 seconds.
     */
    public static final int DEFAULT_ZK_CONNECTION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(10);
    /**
     * Default Zookeeper root, '/'.
     */
    public static final String DEFAULT_ZK_ROOT = "/";

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new KafkaRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final String topic = getTopic(conf);
        final String group = getConsumerGroup(conf);
        return getInputSplits(conf, topic, group);
    }

    /**
     * Gets all of the input splits for the {@code topic}, filtering out any {@link InputSplit}s already consumed by the
     * {@code group}.
     * 
     * @param conf
     *            the job configuration.
     * @param topic
     *            the topic.
     * @param group
     *            the consumer group.
     * @return input splits for the job.
     * @throws IOException
     */
    List<InputSplit> getInputSplits(final Configuration conf, final String topic, final String group)
            throws IOException {
        final List<InputSplit> splits = Lists.newArrayList();
        final ZkUtils zk = getZk(conf);
        final Map<Broker, SimpleConsumer> consumers = Maps.newHashMap();
        try {
            for (final Partition partition : zk.getPartitions(topic)) {

                // cache the consumer connections - each partition will make use of each broker consumer
                final Broker broker = partition.getBroker();
                if (!consumers.containsKey(broker)) {
                    consumers.put(broker, getConsumer(broker));
                }

                // grab all valid offsets
                final List<Long> offsets = getOffsets(consumers.get(broker), topic, partition.getPartId(),
                        zk.getLastCommit(group, partition));
                for (int i = 0; i < offsets.size() - 1; i++) {
                    // ( offsets in descending order )
                    final long start = offsets.get(i + 1);
                    final long end = offsets.get(i);
                    // since the offsets are in descending order, the first offset in the list is the largest offset for
                    // the current partition. This split will be in charge of committing the offset for this partition.
                    final boolean partitionCommitter = (i == 0);
                    final InputSplit split = new KafkaInputSplit(partition, start, end, partitionCommitter);
                    LOG.debug("Created input split: " + split);
                    splits.add(split);
                }
            }
        } finally {
            // close resources
            IOUtils.closeQuietly(zk);
            for (final SimpleConsumer consumer : consumers.values()) {
                consumer.close();
            }
        }
        return splits;
    }

    @VisibleForTesting
    List<Long> getOffsets(final SimpleConsumer consumer, final String topic, final int partitionNum,
            final long lastCommit) {
        final long start = consumer.getOffsetsBefore(topic, partitionNum, OffsetRequest.EarliestTime(), 1)[0];
        final long[] offsets = ArrayUtils.add(
                consumer.getOffsetsBefore(topic, partitionNum, OffsetRequest.LatestTime(), Integer.MAX_VALUE), start);

        // note that the offsets are in descending order, and do not contain start
        final List<Long> result = Lists.newArrayList();
        for (final long offset : offsets) {
            if (offset > lastCommit) {
                result.add(offset);
            } else { // we can add the "lastCommit" and break out of loop since offsets are in desc order
                result.add(lastCommit);
                break;
            }
        }
        LOG.debug(String.format("Offsets for %s:%d:%d = %s", consumer.host(), consumer.port(), partitionNum, result));
        return result;
    }

    /*
     * We make the following two methods visible for testing so that we can mock these components out in unit tests
     */

    @VisibleForTesting
    SimpleConsumer getConsumer(final Broker broker) {
        return new SimpleConsumer(broker.getHost(), broker.getPort(), DEFAULT_SOCKET_TIMEOUT, DEFAULT_BUFFER_SIZE);
    }

    @VisibleForTesting
    ZkUtils getZk(final Configuration conf) {
        return new ZkUtils(conf);
    }

    /**
     * Sets the Zookeeper connection string (required).
     * 
     * @param job
     *            the job being configured
     * @param zkConnect
     *            zookeeper connection string.
     */
    public static void setZkConnect(final Job job, final String zkConnect) {
        job.getConfiguration().set("kafka.zk.connect", zkConnect);
    }

    /**
     * Gets the Zookeeper connection string set by {@link #setZkConnect(Job, String)}.
     * 
     * @param conf
     *            the job conf.
     * @return the Zookeeper connection string.
     */
    public static String getZkConnect(final Configuration conf) {
        return conf.get("kafka.zk.connect");
    }

    /**
     * Set the Zookeeper session timeout for Kafka.
     * 
     * @param job
     *            the job being configured.
     * @param sessionTimeout
     *            the session timeout in milliseconds.
     */
    public static void setZkSessionTimeout(final Job job, final int sessionTimeout) {
        job.getConfiguration().setInt("kafka.zk.session.timeout.ms", sessionTimeout);
    }

    /**
     * Gets the Zookeeper session timeout set by {@link #setZkSessionTimeout(Job, int)}, defaulting to
     * {@link #DEFAULT_ZK_SESSION_TIMEOUT} if it has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Zookeeper session timeout.
     */
    public static int getZkSessionTimeout(final Configuration conf) {
        return conf.getInt("kafka.zk.session.timeout.ms", DEFAULT_ZK_SESSION_TIMEOUT);
    }

    /**
     * Set the Zookeeper connection timeout for Zookeeper.
     * 
     * @param job
     *            the job being configured.
     * @param connectionTimeout
     *            the connection timeout in milliseconds.
     */
    public static void setZkConnectionTimeout(final Job job, final int connectionTimeout) {
        job.getConfiguration().setInt("kafka.zk.connection.timeout.ms", connectionTimeout);
    }

    /**
     * Gets the Zookeeper connection timeout set by {@link #setZkConnectionTimeout(Job, int)}, defaulting to
     * {@link #DEFAULT_ZK_CONNECTION_TIMEOUT} if it has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Zookeeper connection timeout.
     */
    public static int getZkConnectionTimeout(final Configuration conf) {
        return conf.getInt("kafka.zk.connection.timeout.ms", DEFAULT_ZK_CONNECTION_TIMEOUT);
    }

    /**
     * Sets the Zookeeper root for Kafka.
     * 
     * @param job
     *            the job being configured.
     * @param root
     *            the zookeeper root path.
     */
    public static void setZkRoot(final Job job, final String root) {
        job.getConfiguration().set("kafka.zk.root", root);
    }

    /**
     * Gets the Zookeeper root of Kafka set by {@link #setZkRoot(Job, String)}, defaulting to {@link #DEFAULT_ZK_ROOT}
     * if it has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Zookeeper root of Kafka.
     */
    public static String getZkRoot(final Configuration conf) {
        return conf.get("kafka.zk.root", DEFAULT_ZK_ROOT);
    }

    /**
     * Sets the input topic (required).
     * 
     * @param job
     *            the job being configured
     * @param topic
     *            the topic name
     */
    public static void setTopic(final Job job, final String topic) {
        job.getConfiguration().set("kafka.topic", topic);
    }

    /**
     * Gets the input topic.
     * 
     * @param conf
     *            the job conf.
     * @return the input topic.
     */
    public static String getTopic(final Configuration conf) {
        return conf.get("kafka.topic");
    }

    /**
     * Sets the consumer group of the input reader (required).
     * 
     * @param job
     *            the job being configured.
     * @param consumerGroup
     *            consumer group name.
     */
    public static void setConsumerGroup(final Job job, final String consumerGroup) {
        job.getConfiguration().set("kafka.groupid", consumerGroup);
    }

    /**
     * Gets the consumer group.
     * 
     * @param conf
     *            the job conf.
     * @return the consumer group.
     */
    public static String getConsumerGroup(final Configuration conf) {
        return conf.get("kafka.groupid");
    }

    /**
     * Sets the fetch size of the {@link RecordReader}. Note that your mapper should have enough memory allocation to
     * handle the specified size, or else you will likely throw {@link OutOfMemoryError}s.
     * 
     * @param job
     *            the job being configured.
     * @param fetchSize
     *            the fetch size (bytes).
     */
    public static void setFetchSize(final Job job, final int fetchSize) {
        job.getConfiguration().setInt("kafka.fetch.size", fetchSize);
    }

    /**
     * Gets the Kafka fetch size set by {@link #setFetchSize(Job, int)}, defaulting to {@link #DEFAULT_FETCH_SIZE} if it
     * has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Kafka fetch size.
     */
    public static int getFetchSize(final Configuration conf) {
        return conf.getInt("kafka.fetch.size", DEFAULT_FETCH_SIZE);
    }

    /**
     * Sets the buffer size of the {@link SimpleConsumer} inside of the {@link KafkaRecordReader}.
     * 
     * @param job
     *            the job being configured.
     * @param bufferSize
     *            the buffer size (bytes).
     */
    public static void setBufferSize(final Job job, final int bufferSize) {
        job.getConfiguration().setInt("kafka.socket.buffersize", bufferSize);
    }

    /**
     * Gets the Kafka buffer size set by {@link #setBufferSize(Job, int)}, defaulting to {@link #DEFAULT_BUFFER_SIZE} if
     * it has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Kafka buffer size.
     */
    public static int getBufferSize(final Configuration conf) {
        return conf.getInt("kafka.socket.buffersize", DEFAULT_BUFFER_SIZE);
    }

    /**
     * Sets the socket timeout of the {@link SimpleConsumer} inside of the {@link KafkaRecordReader}.
     * 
     * @param job
     *            the job being configured.
     * @param timeout
     *            the socket timeout (milliseconds).
     */
    public static void setSocketTimeout(final Job job, final int timeout) {
        job.getConfiguration().setInt("kafka.socket.timeout.ms", timeout);
    }

    /**
     * Gets the Kafka socket timeout set by {@link #setSocketTimeout(Job, int)}, defaulting to
     * {@link #DEFAULT_SOCKET_TIMEOUT} if it has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Kafka socket timeout.
     */
    public static int getSocketTimeout(final Configuration conf) {
        return conf.getInt("kafka.socket.timeout.ms", DEFAULT_SOCKET_TIMEOUT);
    }
}
