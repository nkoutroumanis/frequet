package gr.ds.unipi.spatialnodb.messages.common.vre;

//import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
//import fi.iki.yak.ts.compression.gorilla.Compressor;
//import gr.aueb.delorean.chimp.ChimpN;
//import gr.aueb.delorean.chimp.ChimpNNoIndex;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class VRERecordWithKeyWriteSupport extends WriteSupport<VRERecordWithKey> {

    MessageType schema = MessageTypeParser.parseMessageType( "message VRERecord {\n" +
            "required BINARY objectId;\n" +
            "required INT32 serialNumber;\n" +
            "required INT32 segmentType;\n" +
            "required BINARY longitude;\n" +
            "required BINARY latitude;\n" +
            "required DOUBLE minLongitude;\n" +
            "required DOUBLE minLatitude;\n" +
            "required DOUBLE maxLongitude;\n" +
            "required DOUBLE maxLatitude;\n" +
            "required BINARY firstLastLongitude;\n" +
            "required BINARY firstLastLatitude;\n" +
            "required BINARY signature;\n" +
            "required INT64 key;\n" +
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
    public void write(VRERecordWithKey trajectory) {
        recordConsumer.startMessage();

        recordConsumer.startField("objectId",0);
        recordConsumer.addBinary(Binary.fromString(trajectory.getRecord().getObjectId()));
        recordConsumer.endField("objectId",0);

        recordConsumer.startField("serialNumber",1);
        recordConsumer.addInteger(trajectory.getRecord().getSerialNumber());
        recordConsumer.endField("serialNumber",1);

        recordConsumer.startField("segmentType",2);
        recordConsumer.addInteger(trajectory.getRecord().getSegmentType());
        recordConsumer.endField("segmentType",2);

        ByteBuffer blongitude = ByteBuffer.allocate(trajectory.getRecord().getSpatialPoints().length*8);
        for (SpatialPoint stPoint : trajectory.getRecord().getSpatialPoints()) {
            blongitude.putDouble(stPoint.getLongitude());
        }

        recordConsumer.startField("longitude",3);
        recordConsumer.addBinary(Binary.fromConstantByteArray(blongitude.array()));
        recordConsumer.endField("longitude",3);

        ByteBuffer blatitude = ByteBuffer.allocate(trajectory.getRecord().getSpatialPoints().length*8);
        for (SpatialPoint stPoint : trajectory.getRecord().getSpatialPoints()) {
            blatitude.putDouble(stPoint.getLatitude());
        }
        recordConsumer.startField("latitude",4);
        recordConsumer.addBinary(Binary.fromConstantByteArray(blatitude.array()));
        recordConsumer.endField("latitude",4);

        recordConsumer.startField("minLongitude",5);
        recordConsumer.addDouble(trajectory.getRecord().getMinLongitude());
        recordConsumer.endField("minLongitude",5);

        recordConsumer.startField("minLatitude",6);
        recordConsumer.addDouble(trajectory.getRecord().getMinLatitude());
        recordConsumer.endField("minLatitude",6);

        recordConsumer.startField("maxLongitude",7);
        recordConsumer.addDouble(trajectory.getRecord().getMaxLongitude());
        recordConsumer.endField("maxLongitude",7);

        recordConsumer.startField("maxLatitude",8);
        recordConsumer.addDouble(trajectory.getRecord().getMaxLatitude());
        recordConsumer.endField("maxLatitude",8);

        ByteBuffer firstLastLongitude = ByteBuffer.allocate(2*8);
        firstLastLongitude.putDouble(trajectory.getRecord().getFirstPoint().getLongitude());
        firstLastLongitude.putDouble(trajectory.getRecord().getLastPoint().getLongitude());

        recordConsumer.startField("firstLastLongitude",9);
        recordConsumer.addBinary(Binary.fromConstantByteArray(firstLastLongitude.array()));
        recordConsumer.endField("firstLastLongitude",9);

        ByteBuffer firstLastLatitude = ByteBuffer.allocate(2*8);
        firstLastLatitude.putDouble(trajectory.getRecord().getFirstPoint().getLatitude());
        firstLastLatitude.putDouble(trajectory.getRecord().getLastPoint().getLatitude());

        recordConsumer.startField("firstLastLatitude",10);
        recordConsumer.addBinary(Binary.fromConstantByteArray(firstLastLatitude.array()));
        recordConsumer.endField("firstLastLatitude",10);

        recordConsumer.startField("signature",11);
        recordConsumer.addBinary(Binary.fromConstantByteArray(trajectory.getRecord().getSignature()));
        recordConsumer.endField("signature",11);

        recordConsumer.startField("key",12);
        recordConsumer.addLong(trajectory.getKey());
        recordConsumer.endField("key",12);

        recordConsumer.endMessage();
    }
}
