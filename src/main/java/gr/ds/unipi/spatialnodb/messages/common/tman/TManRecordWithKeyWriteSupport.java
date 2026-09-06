package gr.ds.unipi.spatialnodb.messages.common.tman;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class TManRecordWithKeyWriteSupport extends WriteSupport<TManRecordWithKey> {

    MessageType schema = MessageTypeParser.parseMessageType( "message TManRecord {\n" +
            "required BINARY objectId;\n" +
            "required BINARY longitude;\n" +
            "required BINARY latitude;\n" +
            "required INT64 key;\n" +
            "repeated INT32 dpPointIndexes;\n" +
            "required BINARY dpMbrs;\n" +
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
    public void write(TManRecordWithKey trajectory) {
        recordConsumer.startMessage();

        recordConsumer.startField("objectId",0);
        recordConsumer.addBinary(Binary.fromString(trajectory.getRecord().getObjectId()));
        recordConsumer.endField("objectId",0);

        ByteBuffer blongitude = ByteBuffer.allocate(trajectory.getRecord().getSpatialPoints().length*8);
        for (SpatialPoint stPoint : trajectory.getRecord().getSpatialPoints()) {
            blongitude.putDouble(stPoint.getLongitude());
        }
        recordConsumer.startField("longitude",1);
        recordConsumer.addBinary(Binary.fromConstantByteArray(blongitude.array()));
        recordConsumer.endField("longitude",1);

        ByteBuffer blatitude = ByteBuffer.allocate(trajectory.getRecord().getSpatialPoints().length*8);
        for (SpatialPoint stPoint : trajectory.getRecord().getSpatialPoints()) {
            blatitude.putDouble(stPoint.getLatitude());
        }
        recordConsumer.startField("latitude",2);
        recordConsumer.addBinary(Binary.fromConstantByteArray(blatitude.array()));
        recordConsumer.endField("latitude",2);

        recordConsumer.startField("key",3);
        recordConsumer.addLong(trajectory.getKey());
        recordConsumer.endField("key",3);

        recordConsumer.startField("dpPointIndexes",4);
        for (int dpPointIndex : trajectory.getRecord().getDpPointIndexes()) {
            recordConsumer.addInteger(dpPointIndex);
        }
        recordConsumer.endField("dpPointIndexes",4);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(trajectory.getRecord().getDpMbrs());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        byte[] bytes = baos.toByteArray();

        recordConsumer.startField("dpMbrs",5);
        recordConsumer.addBinary(Binary.fromConstantByteArray(bytes));
        recordConsumer.endField("dpMbrs",5);

        recordConsumer.endMessage();
    }
}
