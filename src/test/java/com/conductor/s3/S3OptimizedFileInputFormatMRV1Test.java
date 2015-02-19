package com.conductor.s3;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.junit.Test;

/**
 * Since most logic is covered in {@link com.conductor.s3.S3InputFormatUtilsTest}, these tests are pretty bare.
 * 
 * @author cgreen
 */
public class S3OptimizedFileInputFormatMRV1Test {

    private S3OptimizedFileInputFormatMRV1 inputFormat = new S3OptimizedFileInputFormatMRV1() {
        @Override
        public RecordReader getRecordReader(final InputSplit split, final JobConf job, final Reporter reporter)
                throws IOException {
            throw new UnsupportedOperationException("NOT SUPPORTED!");
        }
    };

    @Test(expected = IOException.class)
    public void testListStatusesNoInputDirs() throws Exception {
        inputFormat.listStatus(new JobConf(false));
    }

    @Test
    public void testGetSplits() throws Exception {
        final S3OptimizedFileInputFormatMRV1 ifSpy = spy(inputFormat);
        final JobConf conf = new JobConf(false);

        final FileStatus fs = new FileStatus(100, false, 1, 100, 1l, new Path("s3n://bucket/blah/blah/blah"));
        doReturn(new FileStatus[] { fs }).when(ifSpy).listStatus(conf);

        final InputSplit[] splits = ifSpy.getSplits(conf, 10);
        assertEquals(1, splits.length);
        assertEquals(1, conf.getLong("mapreduce.input.num.files", -1));
    }
}
