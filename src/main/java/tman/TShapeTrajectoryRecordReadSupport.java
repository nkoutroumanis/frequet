package tman;/*
 * TShapeTrajectoryRecordReadSupport.java
 * =======================================
 *
 * Mirrors the user's TrajectorySegmentReadSupport exactly: parses the same
 * schema string as the WriteSupport (kept as a single shared constant,
 * TShapeTrajectoryRecordWriteSupport.SCHEMA, so the two can never drift
 * apart -- the original TrajectorySegment{Read,Write}Support instead
 * duplicate the schema string in both places) and hands back a
 * TShapeTrajectoryRecordMaterializer to actually build each record.
 *
 * This is the class you pass to ParquetInputFormat.setReadSupportClass(...)
 * before reading (see TShapeFrechetQuery.java).
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class TShapeTrajectoryRecordReadSupport extends ReadSupport<TShapeTrajectoryRecord> {

    @Override
    public ReadContext init(InitContext context) {
        return new ReadContext(TShapeTrajectoryRecordWriteSupport.SCHEMA);
    }

    @Override
    public RecordMaterializer<TShapeTrajectoryRecord> prepareForRead(
            Configuration configuration,
            Map<String, String> keyValueMetaData,
            MessageType fileSchema,
            ReadContext readContext) {
        return new TShapeTrajectoryRecordMaterializer();
    }
}
