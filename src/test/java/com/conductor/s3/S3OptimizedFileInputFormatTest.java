package com.conductor.s3;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * @author cgreen
 */
public class S3OptimizedFileInputFormatTest {

    private S3OptimizedFileInputFormat inputFormat = new S3OptimizedFileInputFormat() {

        @Override
        public RecordReader createRecordReader(final InputSplit split, final TaskAttemptContext context)
                throws IOException, InterruptedException {
            throw new UnsupportedOperationException("NOT SUPPORTED");
        }
    };

    private JobContext job = mock(JobContext.class);
    private Configuration conf = new Configuration(false);

    @Before
    public void setUp() throws Exception {
        when(job.getConfiguration()).thenReturn(conf);
    }

    @Test(expected = IOException.class)
    public void testListStatusesNoInputDirs() throws Exception {
        inputFormat.getSplits(job);
    }

    @Test
    public void testGetSplits() throws Exception {
        final S3OptimizedFileInputFormat ifSpy = spy(inputFormat);

        final FileStatus fs = new FileStatus(100, false, 1, 100, 1l, new Path("s3n://bucket/blah/blah/blah"));
        doReturn(Lists.newArrayList(fs)).when(ifSpy).listStatus(job);

        final List<InputSplit> splits = ifSpy.getSplits(job);
        assertEquals(1, splits.size());
    }
}
