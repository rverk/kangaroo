package com.conductor.kafka.hadoop;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.conductor.kafka.Broker;
import com.conductor.kafka.Partition;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

/**
 * @author cgreen
 */
public class KafkaInputSplitTest {

    @Test
    public void testSerialization() throws Exception {
        final Broker broker = new Broker("127.0.0.1", 9092, 1);
        final Partition partition = new Partition("topic_name", 0, broker);
        final KafkaInputSplit split = new KafkaInputSplit(partition, 0, 10l, false);
        final ByteArrayDataOutput out = ByteStreams.newDataOutput();
        split.write(out);

        final KafkaInputSplit actual = new KafkaInputSplit();
        actual.readFields(ByteStreams.newDataInput(out.toByteArray()));

        assertEquals(split, actual);
    }
}
