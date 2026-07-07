package gr.ds.unipi.spatialnodb.messages.common.trajparquet.pathReadParquet;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;

import java.io.IOException;

public class ParquetRecordReaderWrapper<V> extends CombineFileRecordReaderWrapper<Void, V> {

    /**
     * CRITICAL: Do not alter this constructor signature.
     * Hadoop's internal CombineFileRecordReader uses precise Java Reflection mapping
     * and strictly requires this exact parameter layout (CombineFileSplit, TaskAttemptContext, Integer).
     */
    public ParquetRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer idx)
            throws IOException, InterruptedException {

        // Instantiates a clean, native ParquetInputFormat instance.
        // Types map cleanly to FileInputFormat<Void, V> without requiring explicit casts.
        super(new ParquetInputFormat<V>(), split, context, idx);
    }
}