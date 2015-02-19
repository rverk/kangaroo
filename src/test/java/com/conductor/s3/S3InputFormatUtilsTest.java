package com.conductor.s3;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.google.common.collect.Lists;

/**
 * @author cgreen
 */
public class S3InputFormatUtilsTest {

    @Test
    public void testGetFileStatuses() throws Exception {

        final AmazonS3 client = mock(AmazonS3.class);

        // listing 1 of first path
        final ObjectListing listing1 = mock(ObjectListing.class);
        final S3ObjectSummary s11 = new S3ObjectSummary();
        s11.setBucketName("my-bucket");
        s11.setKey("first/path/part-1");
        s11.setSize(999);
        s11.setLastModified(new Date(99));
        final S3ObjectSummary s12 = new S3ObjectSummary();
        s12.setBucketName("my-bucket");
        s12.setKey("first/path/part-2");
        s12.setSize(888);
        s12.setLastModified(new Date(88));
        final List<S3ObjectSummary> listings1 = Lists.newArrayList(s11, s12);
        when(listing1.getObjectSummaries()).thenReturn(listings1);
        when(listing1.isTruncated()).thenReturn(true);

        // listing 2 of first path
        final ObjectListing listing2 = mock(ObjectListing.class);
        final S3ObjectSummary s21 = new S3ObjectSummary();
        s21.setBucketName("my-bucket");
        s21.setKey("first/path/part-3");
        s21.setSize(777);
        s21.setLastModified(new Date(77));
        final List<S3ObjectSummary> listings2 = Lists.newArrayList(s21);
        when(listing2.getObjectSummaries()).thenReturn(listings2);
        when(listing2.isTruncated()).thenReturn(false);

        // listing 1 of second path -- all of these should get filtered
        final ObjectListing listing3 = mock(ObjectListing.class);
        final S3ObjectSummary s31 = new S3ObjectSummary();
        s31.setBucketName("my-bucket");
        s31.setKey("second/path/.secret");
        s31.setSize(666);
        s31.setLastModified(new Date(66));
        final S3ObjectSummary s32 = new S3ObjectSummary();
        s32.setBucketName("my-bucket");
        s32.setKey("second/path/_other_secret");
        s32.setSize(555);
        s32.setLastModified(new Date(55));
        final S3ObjectSummary s33 = new S3ObjectSummary();
        s33.setBucketName("my-bucket");
        s33.setKey("second/path/my_$folder$");
        s33.setSize(444);
        s33.setLastModified(new Date(44));
        final List<S3ObjectSummary> listings3 = Lists.newArrayList(s31, s32, s33);
        when(listing3.getObjectSummaries()).thenReturn(listings3);
        when(listing3.isTruncated()).thenReturn(false);

        when(client.listObjects(any(ListObjectsRequest.class))).thenAnswer(new Answer<ObjectListing>() {
            @Override
            public ObjectListing answer(final InvocationOnMock invocation) throws Throwable {
                final ListObjectsRequest req = (ListObjectsRequest) invocation.getArguments()[0];
                assertEquals("my-bucket", req.getBucketName());
                assertEquals("first/path", req.getPrefix());
                assertEquals(Integer.MAX_VALUE, (int) req.getMaxKeys());
                return listing1;
            }
        }).thenAnswer(new Answer<ObjectListing>() {
            @Override
            public ObjectListing answer(final InvocationOnMock invocation) throws Throwable {
                final ListObjectsRequest req = (ListObjectsRequest) invocation.getArguments()[0];
                assertEquals("my-bucket", req.getBucketName());
                assertEquals("second/path", req.getPrefix());
                assertEquals(Integer.MAX_VALUE, (int) req.getMaxKeys());
                return listing3;
            }
        });

        when(client.listNextBatchOfObjects(listing1)).thenReturn(listing2);

        final List<FileStatus> result = S3InputFormatUtils.getFileStatuses(client, 100, new Path(
                "s3n://my-bucket/first/path"), new Path("s3n://my-bucket/second/path"));

        verify(client, times(2)).listObjects(any(ListObjectsRequest.class));
        verify(client, times(1)).listNextBatchOfObjects(any(ObjectListing.class));

        assertEquals(3, result.size());

        final FileStatus fs1 = result.get(0);
        assertEquals("s3n://my-bucket/first/path/part-1", fs1.getPath().toString());
        assertEquals(100l, fs1.getBlockSize());
        assertEquals(999l, fs1.getLen());
        assertEquals(99l, fs1.getModificationTime());

        final FileStatus fs2 = result.get(1);
        assertEquals("s3n://my-bucket/first/path/part-2", fs2.getPath().toString());
        assertEquals(100l, fs2.getBlockSize());
        assertEquals(888l, fs2.getLen());
        assertEquals(88l, fs2.getModificationTime());

        final FileStatus fs3 = result.get(2);
        assertEquals("s3n://my-bucket/first/path/part-3", fs3.getPath().toString());
        assertEquals(100l, fs3.getBlockSize());
        assertEquals(777l, fs3.getLen());
        assertEquals(77l, fs3.getModificationTime());
    }

    @Test
    public void testConvertToInputSplits() throws Exception {
        final FileStatus fs1 = new FileStatus(999l, false, 1, 1000l, 99l, new Path("s3n://my-bucket/first/path/part-1"));
        final FileStatus fs2 = new FileStatus(880l, false, 1, 1000l, 88l, new Path("s3n://my-bucket/first/path/part-2"));
        final FileStatus fs3 = new FileStatus(777l, false, 1, 1000l, 77l, new Path("s3n://my-bucket/first/path/part-3"));
        final FileStatus fs4 = new FileStatus(4l, false, 1, 1000l, 77l, new Path("s3n://my-bucket/first/path/part-4"));

        final List<InputSplit> inputSplits = S3InputFormatUtils.convertToInputSplitsMRV1(
                Lists.newArrayList(fs1, fs2, fs3, fs4), 5, 800);

        assertEquals(5, inputSplits.size());

        final FileSplit split1 = (FileSplit) inputSplits.get(0);
        assertEquals("s3n://my-bucket/first/path/part-1", split1.getPath().toString());
        assertEquals(0, split1.getStart());
        assertEquals(800, split1.getLength());

        final FileSplit split2 = (FileSplit) inputSplits.get(1);
        assertEquals("s3n://my-bucket/first/path/part-1", split2.getPath().toString());
        assertEquals(800, split2.getStart());
        assertEquals(199, split2.getLength());

        final FileSplit split3 = (FileSplit) inputSplits.get(2);
        assertEquals("s3n://my-bucket/first/path/part-2", split3.getPath().toString());
        assertEquals(0, split3.getStart());
        assertEquals(880, split3.getLength()); // slop!

        final FileSplit split4 = (FileSplit) inputSplits.get(3);
        assertEquals("s3n://my-bucket/first/path/part-3", split4.getPath().toString());
        assertEquals(0, split4.getStart());
        assertEquals(777, split4.getLength());

        final FileSplit split5 = (FileSplit) inputSplits.get(4);
        assertEquals("s3n://my-bucket/first/path/part-4", split5.getPath().toString());
        assertEquals(0, split5.getStart());
        assertEquals(4, split5.getLength());
    }

    @Test
    public void testSplitConverter() throws Exception {
        final Path path = new Path("s3n://my-bucket/first/path/part-4");
        final FileSplit v1Split = new FileSplit(path, 0, 800, new String[0]);
        final org.apache.hadoop.mapreduce.lib.input.FileSplit v2Split = (org.apache.hadoop.mapreduce.lib.input.FileSplit) S3InputFormatUtils.SPLIT_CONVERTER
                .apply(v1Split);

        assertEquals(0, v2Split.getStart());
        assertEquals(800, v2Split.getLength());
        assertEquals(path, v2Split.getPath());
    }
}
