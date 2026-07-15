package tman;/*
 * TShapeTrajectoryRecordMaterializer.java
 * ========================================
 *
 * Mirrors the user's TrajectorySegmentMaterializer field-for-field: one
 * PrimitiveConverter per schema column, wired up through a GroupConverter
 * whose getConverter(i) switches on the field's 0-based schema position,
 * and getCurrentRecord() reassembles the packed longitude/latitude byte
 * arrays back into a Point[] using the same ByteBuffer.getDouble() loop
 * TrajectorySegmentMaterializer uses for SpatialPoint[].
 *
 * (The reference class numbered its converters p0, p2, p3, p5, p6, p8, p9 --
 * an artifact of an earlier version of that class with extra
 * timestamp/interval columns that were later removed without renumbering.
 * This version just uses p0..p7 in schema order, matching the 8 columns in
 * TShapeTrajectoryRecordWriteSupport.SCHEMA.)
 */

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;

import java.nio.ByteBuffer;

public class TShapeTrajectoryRecordMaterializer extends RecordMaterializer<TShapeTrajectoryRecord> {

    private String objectId;
    private byte[] longitude;
    private byte[] latitude;
    private long tshapeValue;
    private double minLongitude;
    private double minLatitude;
    private double maxLongitude;
    private double maxLatitude;

    private final GroupConverter groupConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int fieldIndex) {
            switch (fieldIndex) {
                case 0: return p0objectId;
                case 1: return p1longitude;
                case 2: return p2latitude;
                case 3: return p3tshapeValue;
                case 4: return p4minLongitude;
                case 5: return p5minLatitude;
                case 6: return p6maxLongitude;
                case 7: return p7maxLatitude;
                default: return null;
            }
        }

        @Override
        public void start() {
        }

        @Override
        public void end() {
        }
    };

    private final PrimitiveConverter p0objectId = new PrimitiveConverter() {
        @Override
        public void addBinary(Binary value) {
            objectId = value.toStringUsingUTF8();
        }
    };

    private final PrimitiveConverter p1longitude = new PrimitiveConverter() {
        @Override
        public void addBinary(Binary value) {
            longitude = value.getBytes();
        }
    };

    private final PrimitiveConverter p2latitude = new PrimitiveConverter() {
        @Override
        public void addBinary(Binary value) {
            latitude = value.getBytes();
        }
    };

    private final PrimitiveConverter p3tshapeValue = new PrimitiveConverter() {
        @Override
        public void addLong(long value) {
            tshapeValue = value;
        }
    };

    private final PrimitiveConverter p4minLongitude = new PrimitiveConverter() {
        @Override
        public void addDouble(double value) {
            minLongitude = value;
        }
    };

    private final PrimitiveConverter p5minLatitude = new PrimitiveConverter() {
        @Override
        public void addDouble(double value) {
            minLatitude = value;
        }
    };

    private final PrimitiveConverter p6maxLongitude = new PrimitiveConverter() {
        @Override
        public void addDouble(double value) {
            maxLongitude = value;
        }
    };

    private final PrimitiveConverter p7maxLatitude = new PrimitiveConverter() {
        @Override
        public void addDouble(double value) {
            maxLatitude = value;
        }
    };

    @Override
    public TShapeTrajectoryRecord getCurrentRecord() {
        ByteBuffer bLongitude = ByteBuffer.wrap(longitude);
        ByteBuffer bLatitude = ByteBuffer.wrap(latitude);

        int numPoints = longitude.length / 8;
        Point[] points = new Point[numPoints];
        for (int i = 0; i < numPoints; i++) {
            points[i] = new Point(bLongitude.getDouble(), bLatitude.getDouble());
        }

        return new TShapeTrajectoryRecord(
                objectId, points, tshapeValue,
                minLongitude, minLatitude, maxLongitude, maxLatitude);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
