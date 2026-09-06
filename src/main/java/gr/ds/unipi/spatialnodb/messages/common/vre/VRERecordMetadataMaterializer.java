package gr.ds.unipi.spatialnodb.messages.common.vre;

//import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
//import fi.iki.yak.ts.compression.gorilla.Decompressor;
//import fi.iki.yak.ts.compression.gorilla.Value;
//import gr.aueb.delorean.chimp.ChimpNDecompressor;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import org.apache.parquet.io.api.*;

import java.nio.ByteBuffer;

public class VRERecordMetadataMaterializer extends RecordMaterializer<VRERecord> {

    private String objectId;
    private int serialNumber;
    private int segmentType;

    private double minLongitude;
    private double minLatitude;
    private double maxLongitude;
    private double maxLatitude;

    private byte[] firstLastPointLongitude;
    private byte[] firstLastPointLatitude;

    private byte[] signature;

    GroupConverter groupConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i==0){
                return p0;
            } else if(i==1){
                return p1;
            } else if(i==2){
                return p2;
            } else if(i==3){
                return p5;
            } else if(i==4){
                return p6;
            } else if(i==5){
                return p7;
            } else if(i==6){
                return p8;
            } else if(i==7){
                return p3;
            } else if (i==8) {
                return p4;
            } else if(i==9){
                return p11;
            } else if(i==10){
                return p12;
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

    PrimitiveConverter p2 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addInt(int value) {
            segmentType = value;
        }
    };

    PrimitiveConverter p5 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            minLongitude = value;
        }
    };

    PrimitiveConverter p6 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            minLatitude = value;
        }
    };

    PrimitiveConverter p7 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxLongitude = value;
        }
    };

    PrimitiveConverter p8 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxLatitude = value;
        }
    };

    PrimitiveConverter p3 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            firstLastPointLongitude = val.getBytes();
        }
    };

    PrimitiveConverter p4 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            firstLastPointLatitude = val.getBytes();
        }
    };

    PrimitiveConverter p11 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            signature = val.getBytes();
        }
    };

    PrimitiveConverter p12 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
        }
    };


    @Override
    public VRERecord getCurrentRecord() {
        ByteBuffer bLongitude = ByteBuffer.wrap(firstLastPointLongitude);
        ByteBuffer bLatitude = ByteBuffer.wrap(firstLastPointLatitude);

        SpatialPoint firstPoint = new SpatialPoint(bLongitude.getDouble(),bLatitude.getDouble());
        SpatialPoint lastPoint = new SpatialPoint(bLongitude.getDouble(),bLatitude.getDouble());

        return new VRERecord(objectId, serialNumber, segmentType, null, minLongitude, minLatitude, maxLongitude, maxLatitude, firstPoint, lastPoint, signature);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
