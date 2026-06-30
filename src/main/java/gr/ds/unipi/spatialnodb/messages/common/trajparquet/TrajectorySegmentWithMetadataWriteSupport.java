package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

//import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
//import fi.iki.yak.ts.compression.gorilla.Compressor;
//import gr.aueb.delorean.chimp.ChimpN;
//import gr.aueb.delorean.chimp.ChimpNNoIndex;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import gr.ds.unipi.spatialnodb.messages.common.SpatioTemporalPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class TrajectorySegmentWithMetadataWriteSupport extends WriteSupport<TrajectorySegmentWithMetadata> {

    MessageType schema = MessageTypeParser.parseMessageType( "message TrajectorySegment {\n" +
            "required BINARY objectId;\n" +
            "required BINARY longitude;\n" +
            "required BINARY latitude;\n" +
            "required DOUBLE minLongitude;\n" +
            "required DOUBLE minLatitude;\n" +
            "required DOUBLE maxLongitude;\n" +
            "required DOUBLE maxLatitude;\n" +
            "optional BINARY pivotsLongitude;\n" +
            "optional BINARY pivotsLatitude;\n" +
            "optional INT64 intervalStart;\n" +
            "optional INT64 intervalEnd;\n" +
            "}");
    RecordConsumer recordConsumer;

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(TrajectorySegmentWithMetadata trajectory) {
        recordConsumer.startMessage();

        recordConsumer.startField("objectId",0);
        recordConsumer.addBinary(Binary.fromString(trajectory.getTrajectorySegment().getObjectId()));
        recordConsumer.endField("objectId",0);

        ByteBuffer blongitude = ByteBuffer.allocate(trajectory.getTrajectorySegment().getSpatialPoints().length*8);
        for (SpatialPoint stPoint : trajectory.getTrajectorySegment().getSpatialPoints()) {
            blongitude.putDouble(stPoint.getLongitude());
        }
        recordConsumer.startField("longitude",1);
        recordConsumer.addBinary(Binary.fromConstantByteArray(blongitude.array()));
        recordConsumer.endField("longitude",1);

        ByteBuffer blatitude = ByteBuffer.allocate(trajectory.getTrajectorySegment().getSpatialPoints().length*8);
        for (SpatialPoint stPoint : trajectory.getTrajectorySegment().getSpatialPoints()) {
            blatitude.putDouble(stPoint.getLatitude());
        }
        recordConsumer.startField("latitude",2);
        recordConsumer.addBinary(Binary.fromConstantByteArray(blatitude.array()));
        recordConsumer.endField("latitude",2);

        recordConsumer.startField("minLongitude",3);
        recordConsumer.addDouble(trajectory.getTrajectorySegment().getMinLongitude());
        recordConsumer.endField("minLongitude",3);

        recordConsumer.startField("minLatitude",4);
        recordConsumer.addDouble(trajectory.getTrajectorySegment().getMinLatitude());
        recordConsumer.endField("minLatitude",4);

        recordConsumer.startField("maxLongitude",5);
        recordConsumer.addDouble(trajectory.getTrajectorySegment().getMaxLongitude());
        recordConsumer.endField("maxLongitude",5);

        recordConsumer.startField("maxLatitude",6);
        recordConsumer.addDouble(trajectory.getTrajectorySegment().getMaxLatitude());
        recordConsumer.endField("maxLatitude",6);

        if(trajectory.getPivots()!=null) {
            ByteBuffer bPivotlongitude = ByteBuffer.allocate(trajectory.getPivots().length * 8);
            for (SpatialPoint stPoint : trajectory.getPivots()) {
                bPivotlongitude.putDouble(stPoint.getLongitude());
            }
            recordConsumer.startField("pivotsLongitude", 7);
            recordConsumer.addBinary(Binary.fromConstantByteArray(bPivotlongitude.array()));
            recordConsumer.endField("pivotsLongitude", 7);

            ByteBuffer bPivotlatitude = ByteBuffer.allocate(trajectory.getPivots().length * 8);
            for (SpatialPoint stPoint : trajectory.getPivots()) {
                bPivotlatitude.putDouble(stPoint.getLatitude());
            }
            recordConsumer.startField("pivotsLatitude", 8);
            recordConsumer.addBinary(Binary.fromConstantByteArray(bPivotlatitude.array()));
            recordConsumer.endField("pivotsLatitude", 8);
        }
        if(trajectory.getInterval()!=null){
            recordConsumer.startField("intervalStart",9);
            recordConsumer.addLong(trajectory.getInterval()[0]);
            recordConsumer.endField("intervalStart",9);

            recordConsumer.startField("intervalEnd",10);
            recordConsumer.addLong(trajectory.getInterval()[1]);
            recordConsumer.endField("intervalEnd",10);
        }

        recordConsumer.endMessage();
    }
}
