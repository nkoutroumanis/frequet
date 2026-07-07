package gr.ds.unipi.spatialnodb.messages.common.trajparquet.pathReadParquet;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class CombineParquetInputFormat<V> extends CombineFileInputFormat<Void, V> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public RecordReader<Void, V> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {

        // Hand off tracking to the standard Hadoop CombineFileRecordReader.
        // We explicitly cast the wrapper class literal to bypass strict Java generic limits.
        return new CombineFileRecordReader<>(
                (CombineFileSplit) split,
                context,
                (Class) ParquetRecordReaderWrapper.class
        );
    }
}