package com.conductor.kafka.hadoop;

import static com.conductor.kafka.hadoop.KafkaInputFormat.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import kafka.api.FetchRequest;
import kafka.common.ErrorMapping;
import kafka.consumer.SimpleConsumer;
import kafka.message.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;

import scala.collection.Iterator;

import com.conductor.zk.ZkUtils;
import com.google.common.annotations.VisibleForTesting;

/**
 * A record reader that reads a subsection, [{@link #getStart()}, {@link #getEnd()}), of a Kafka queue
 * {@link com.conductor.kafka.Partition}.
 * 
 * <p/>
 * Thanks to <a href="https://github.com/miniway">Dongmin Yu</a> for providing the inspiration for this code.
 * 
 * <p/>
 * The original source code can be found <a target="_blank" href="https://github.com/miniway/kafka-hadoop-consumer">on
 * Github</a>.
 * 
 * @see KafkaInputSplit
 * @see KafkaInputFormat
 * @see MultipleKafkaInputFormat
 * 
 * @author <a href="mailto:cgreen@conductor.com">Casey Green</a>
 */
public class KafkaRecordReader extends RecordReader<LongWritable, BytesWritable> {
    private static final Log LOG = LogFactory.getLog(KafkaRecordReader.class);

    private Configuration conf;
    private KafkaInputSplit split;
    private SimpleConsumer consumer;
    private Iterator<MessageAndOffset> currentMessageItr;
    private LongWritable key;
    private BytesWritable value;
    private long start;
    private long end;
    private long pos;
    private int fetchSize;
    private long currentOffset;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException,
            InterruptedException {
        if (!(split instanceof KafkaInputSplit)) {
            throw new IllegalArgumentException("Expected an InputSplit of type KafkaInputSplit but got "
                    + split.getClass());
        }

        final KafkaInputSplit inputSplit = (KafkaInputSplit) split;
        this.conf = context.getConfiguration();
        this.split = inputSplit;
        this.start = inputSplit.getStartOffset();
        this.pos = inputSplit.getStartOffset();
        this.currentOffset = inputSplit.getStartOffset();
        this.end = inputSplit.getEndOffset();
        this.fetchSize = KafkaInputFormat.getFetchSize(conf);
        this.consumer = getConsumer(inputSplit, conf);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        consumer.close();
        if (split.isPartitionCommitter()) {
            commitOffset();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (pos >= end || start == end) {
            return 1.0f;
        }
        return Math.min(1.0f, (pos - start) / (float) (end - start));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new BytesWritable();
        }
        if (continueItr()) {
            final MessageAndOffset msg = getCurrentMessageItr().next();
            final long msgOffset = msg.offset();
            final Message message = msg.message();
            final ByteBuffer buffer = message.payload();
            value.set(buffer.array(), buffer.arrayOffset(), message.payloadSize());
            key.set(msgOffset);
            pos = msgOffset;
            return true;
        }
        return false;
    }

    /**
     * THIS METHOD HAS SIDE EFFECTS - it will update {@code currentMessageItr} (if necessary) and then return true iff
     * the iterator still has elements to be read. If you call {@link scala.collection.Iterator#next()} when this method
     * returns false, you risk a {@link NullPointerException} OR a no-more-elements exception.
     * 
     * @return true if you can call {@link scala.collection.Iterator#next()} on {@code currentMessageItr}.
     */
    @VisibleForTesting
    boolean continueItr() {
        final long remaining = end - currentOffset;
        if (!canCallNext() && remaining > 0) {
            final int theFetchSize = (fetchSize > remaining) ? (int) remaining : fetchSize;
            LOG.debug(String.format("%s fetching %d bytes starting at offset %d", split.toString(), theFetchSize,
                    currentOffset));
            final FetchRequest request = new FetchRequest(split.getPartition().getTopic(), split.getPartition()
                    .getPartId(), currentOffset, theFetchSize);
            final ByteBufferMessageSet msg = consumer.fetch(request);
            final int errorCode = msg.getErrorCode();
            if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
                return false;
            }
            if (errorCode != ErrorMapping.NoError()) {
                ErrorMapping.maybeThrowException(errorCode);
            } // --> else we try to grab the next iterator
            currentMessageItr = msg.iterator();
            currentOffset += msg.validBytes();
        }
        return canCallNext();
    }

    @VisibleForTesting
    boolean canCallNext() {
        return getCurrentMessageItr() != null && getCurrentMessageItr().hasNext();
    }

    @VisibleForTesting
    void commitOffset() throws IOException {
        ZkUtils zk = null;
        try {
            zk = getZk();
            // note: last parameter (temp) MUST be true. It is up to the ToolRunner to commit offsets upon successful
            // job execution. Reason: since there are multiple input splits per partition, the consumer group could get
            // into a bad state if this split finished successfully and committed the offset while another input split
            // from the same partition didn't finish successfully.
            zk.setLastCommit(getConsumerGroup(conf), split.getPartition(), currentOffset, true);
        } finally {
            IOUtils.closeQuietly(zk);
        }
    }

    /*
     * We make the following methods visible for testing so that we can mock these components out in unit tests
     */

    @VisibleForTesting
    SimpleConsumer getConsumer(final KafkaInputSplit split, final Configuration conf) {
        return new SimpleConsumer(split.getPartition().getBroker().getHost(), split.getPartition().getBroker()
                .getPort(), getSocketTimeout(conf), getBufferSize(conf));
    }

    @VisibleForTesting
    ZkUtils getZk() {
        return new ZkUtils(conf);
    }

    public Configuration getConf() {
        return conf;
    }

    public KafkaInputSplit getSplit() {
        return split;
    }

    public Iterator<MessageAndOffset> getCurrentMessageItr() {
        return currentMessageItr;
    }

    public long getStart() {
        return start;
    }

    public long getPos() {
        return pos;
    }

    public long getEnd() {
        return end;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }
}
