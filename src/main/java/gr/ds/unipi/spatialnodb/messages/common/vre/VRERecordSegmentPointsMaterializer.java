package gr.ds.unipi.spatialnodb.messages.common.vre;

//import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
//import fi.iki.yak.ts.compression.gorilla.Decompressor;
//import fi.iki.yak.ts.compression.gorilla.Value;
//import gr.aueb.delorean.chimp.ChimpNDecompressor;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import org.apache.parquet.io.api.*;

import java.nio.ByteBuffer;

public class VRERecordSegmentPointsMaterializer extends RecordMaterializer<VRERecord> {

    private String objectId;
    private int serialNumber;

    private byte[] longitude;
    private byte[] latitude;

    GroupConverter groupConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i==0){
                return p0;
            } else if(i==1){
                return p1;
            } else if(i==2){
                return p3;
            } else if(i==3){
                return p4;
            }
            return null;
        }

        @Override
        public void start() {
        }

        @Override
        public void end() {
        }
    };


    PrimitiveConverter p0 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary value) {
            objectId = value.toStringUsingUTF8();
        }
    };

    PrimitiveConverter p1 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addInt(int value) {
            serialNumber = value;
        }
    };


    PrimitiveConverter p3 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            longitude = val.getBytes();
        }
    };

    PrimitiveConverter p4 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            latitude = val.getBytes();
        }
    };

    @Override
    public VRERecord getCurrentRecord() {

        ByteBuffer bLongitude = ByteBuffer.wrap(longitude);
        ByteBuffer bLatitude = ByteBuffer.wrap(latitude);

        SpatialPoint[] spatialPoints = new SpatialPoint[bLongitude.array().length/8];
        for (int i = 0; i < spatialPoints.length; i++) {
            spatialPoints[i] = new SpatialPoint(bLongitude.getDouble(),bLatitude.getDouble());
        }

        return new VRERecord(objectId, serialNumber,  -1, spatialPoints, -1, -1, -1, -1, null, null, null);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
