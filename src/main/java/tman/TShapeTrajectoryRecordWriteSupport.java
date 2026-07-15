package tman;/*
 * TShapeTrajectoryRecordWriteSupport.java
 * ========================================
 *
 * Parquet WriteSupport for TShapeTrajectoryRecord, following exactly the
 * same structure as the user's own TrajectorySegmentWriteSupport: a schema
 * string parsed with MessageTypeParser, and a write() method that streams
 * one field at a time through the RecordConsumer using matching
 * startField(name, index)/...add.../endField(name, index) calls (the index
 * must match the field's position in the schema string, 0-based).
 *
 * The only structural differences from TrajectorySegmentWriteSupport are:
 *   - an added "tshapeValue" INT64 column (see TShapeTrajectoryRecord's
 *     class comment for why it's there), and
 *   - no timestamp/interval columns, since TShape (unlike the segment
 *     scheme this mirrors) has no temporal dimension and never splits a
 *     trajectory across records.
 *
 * Needs org.apache.parquet:parquet-hadoop and org.apache.hadoop:hadoop-*
 * on the classpath (parquet-hadoop.WriteSupport pulls in Hadoop's
 * Configuration even for purely local, non-HDFS usage). See the project
 * notes for the exact jar/versions used elsewhere in this codebase
 * (frequet's pom.xml): parquet-hadoop/parquet-common 1.12.3,
 * parquet-column 1.13.0-trajparquet.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class TShapeTrajectoryRecordWriteSupport extends WriteSupport<TShapeTrajectoryRecord> {

    static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
            "message TShapeTrajectoryRecord {\n" +
            "required BINARY objectId;\n" +
            "required BINARY longitude;\n" +
            "required BINARY latitude;\n" +
            "required INT64 tshapeValue;\n" +
            "required DOUBLE minLongitude;\n" +
            "required DOUBLE minLatitude;\n" +
            "required DOUBLE maxLongitude;\n" +
            "required DOUBLE maxLatitude;\n" +
            "}");

    private RecordConsumer recordConsumer;

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(SCHEMA, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(TShapeTrajectoryRecord record) {
        recordConsumer.startMessage();

        recordConsumer.startField("objectId", 0);
        recordConsumer.addBinary(Binary.fromString(record.getObjectId()));
        recordConsumer.endField("objectId", 0);

        Point[] points = record.getPoints();

        ByteBuffer bLongitude = ByteBuffer.allocate(points.length * 8);
        for (Point p : points) {
            bLongitude.putDouble(p.x);
        }
        recordConsumer.startField("longitude", 1);
        recordConsumer.addBinary(Binary.fromConstantByteArray(bLongitude.array()));
        recordConsumer.endField("longitude", 1);

        ByteBuffer bLatitude = ByteBuffer.allocate(points.length * 8);
        for (Point p : points) {
            bLatitude.putDouble(p.y);
        }
        recordConsumer.startField("latitude", 2);
        recordConsumer.addBinary(Binary.fromConstantByteArray(bLatitude.array()));
        recordConsumer.endField("latitude", 2);

        recordConsumer.startField("tshapeValue", 3);
        recordConsumer.addLong(record.getTshapeValue());
        recordConsumer.endField("tshapeValue", 3);

        recordConsumer.startField("minLongitude", 4);
        recordConsumer.addDouble(record.getMinLongitude());
        recordConsumer.endField("minLongitude", 4);

        recordConsumer.startField("minLatitude", 5);
        recordConsumer.addDouble(record.getMinLatitude());
        recordConsumer.endField("minLatitude", 5);

        recordConsumer.startField("maxLongitude", 6);
        recordConsumer.addDouble(record.getMaxLongitude());
        recordConsumer.endField("maxLongitude", 6);

        recordConsumer.startField("maxLatitude", 7);
        recordConsumer.addDouble(record.getMaxLatitude());
        recordConsumer.endField("maxLatitude", 7);

        recordConsumer.endMessage();
    }
}
