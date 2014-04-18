package com.conductor.kafka.hadoop;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;

import kafka.api.OffsetRequest;
import kafka.consumer.SimpleConsumer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Assert;
import org.junit.Test;

import com.conductor.kafka.Broker;
import com.conductor.kafka.Partition;
import com.conductor.zk.ZkUtils;
import com.google.common.collect.Lists;

/**
 * @author cgreen
 */
public class KafkaInputFormatTest {

    @Test
    public void testGetInputSplits() throws Exception {
        final KafkaInputFormat inputFormat = spy(new KafkaInputFormat());
        final SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
        final ZkUtils mockZk = mock(ZkUtils.class);
        final Configuration mockConf = mock(Configuration.class);

        final Broker broker = new Broker("127.0.0.1", 9092, 1);
        doReturn(mockConsumer).when(inputFormat).getConsumer(broker);
        doReturn(mockZk).when(inputFormat).getZk(mockConf);
        doReturn(Lists.newArrayList(20l, 10l)).when(inputFormat).getOffsets(mockConsumer, "topic", 0, -1);
        doReturn(Lists.newArrayList(30l, 20l, 0l)).when(inputFormat).getOffsets(mockConsumer, "topic", 1, 10);

        final Partition p1 = new Partition("topic", 0, broker);
        final Partition p2 = new Partition("topic", 1, broker);
        when(mockZk.getPartitions("topic")).thenReturn(Lists.newArrayList(p1, p2));
        when(mockZk.getBroker(1)).thenReturn(broker);
        when(mockZk.getLastCommit("group", p1)).thenReturn(-1l);
        when(mockZk.getLastCommit("group", p2)).thenReturn(10l);

        final List<InputSplit> result = inputFormat.getInputSplits(mockConf, "topic", "group");

        // assert the contents of each split
        Assert.assertEquals(3, result.size());
        final KafkaInputSplit split1 = (KafkaInputSplit) result.get(0);
        final Broker broker1 = split1.getPartition().getBroker();
        assertEquals(broker, broker1);
        assertEquals("127.0.0.1", broker1.getHost());
        assertEquals(9092, broker1.getPort());
        assertEquals(1, broker1.getId());
        assertEquals("1-0", split1.getPartition().getBrokerPartition());
        assertEquals(0, split1.getPartition().getPartId());
        assertEquals(10l, split1.getStartOffset());
        assertEquals(20l, split1.getEndOffset());
        assertEquals("topic", split1.getPartition().getTopic());

        final KafkaInputSplit split2 = (KafkaInputSplit) result.get(1);
        assertEquals(20l, split2.getStartOffset());
        assertEquals(30l, split2.getEndOffset());
        assertEquals("1-1", split2.getPartition().getBrokerPartition());

        final KafkaInputSplit split3 = (KafkaInputSplit) result.get(2);
        assertEquals(0l, split3.getStartOffset());
        assertEquals(20l, split3.getEndOffset());
        assertEquals("1-1", split3.getPartition().getBrokerPartition());

        // verify one and only one call to getConsumer - should get the cached consumer second time around
        verify(inputFormat, times(1)).getConsumer(broker);
        verify(inputFormat, times(1)).getConsumer(any(Broker.class));

        // verify the closeable components are closed
        verify(mockConsumer, times(1)).close();
        verify(mockZk, times(1)).close();
    }

    @Test
    public void testGetOffsets() throws Exception {
        final SimpleConsumer consumer = mock(SimpleConsumer.class);
        final int startOffset = 1;
        when(consumer.getOffsetsBefore("topic", 1, OffsetRequest.EarliestTime(), 1)).thenReturn(
                new long[] { startOffset });
        when(consumer.getOffsetsBefore("topic", 1, OffsetRequest.LatestTime(), 1)).thenReturn(new long[] { 100 });

        final long[] offsets = { 101, 91, 81, 71, 61, 51, 41, 31, 21, 11 };
        when(consumer.getOffsetsBefore("topic", 1, OffsetRequest.LatestTime(), Integer.MAX_VALUE)).thenReturn(offsets);

        final KafkaInputFormat inputFormat = new KafkaInputFormat();

        // we should get all offsets back + the start offset with a -1 lastCommit
        final long[] expected = ArrayUtils.add(offsets, startOffset);
        final List<Long> actual = inputFormat.getOffsets(consumer, "topic", 1, -1);
        compareArrayContents(expected, actual);

        // with a lastCommit of 52, we should only get back the first 5 offsets + the lastCommit
        final int lastCommit = 52;
        final long[] expectedWithPreviousCommit = ArrayUtils.add(Arrays.copyOfRange(expected, 0, 5), lastCommit);
        final List<Long> actualWithPreviousCommit = inputFormat.getOffsets(consumer, "topic", 1, lastCommit);
        compareArrayContents(expectedWithPreviousCommit, actualWithPreviousCommit);
    }

    private void compareArrayContents(final long[] expected, final List<Long> actual) {
        Assert.assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; ++i) {
            Assert.assertEquals(format("Expected %d but got %d at index %d", expected[i], actual.get(i), i),
                    expected[i], (long) actual.get(i));
        }
    }
}
