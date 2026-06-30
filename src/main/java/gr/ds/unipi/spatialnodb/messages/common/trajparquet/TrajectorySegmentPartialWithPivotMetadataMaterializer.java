package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

//import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
//import fi.iki.yak.ts.compression.gorilla.Decompressor;
//import fi.iki.yak.ts.compression.gorilla.Value;
//import gr.aueb.delorean.chimp.ChimpNDecompressor;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import org.apache.parquet.io.api.*;

import java.nio.ByteBuffer;

public class TrajectorySegmentPartialWithPivotMetadataMaterializer extends RecordMaterializer<TrajectorySegmentWithPivotMetadata> {

    private String objectId;

    private double minLongitude;
    private double minLatitude;
    private double maxLongitude;
    private double maxLatitude;

    private byte[] pivotsLongitude;
    private byte[] pivotsLatitude;

    GroupConverter groupConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
//            if(i==0){
//                return p0;
//            } else if(i==1){
//                return p1;
//            } else if(i==2){
//                return p2;
//            } else if(i==3){
//                return p3;
//            }else if(i ==4){
//                return p4;
//            } else if(i==5){
//                return p5;
//            } else if(i==6){
//                return p6;
//            } else if(i==7){
//                return p7;
//            } else if(i==8){
//                return p8;
//            } else if(i==9){
//                return p9;
//            } else if(i==10){
//                return p10;
//            }
//            return null;
            if(i==0){
                return p0;
            } else if(i==1){
                return p5;
            } else if(i==2){
                return p6;
            } else if(i==3){
                return p8;
            } else if(i==4){
                return p9;
            }else if(i==5){
                return p11;
            } else if(i==6){
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

//    PrimitiveConverter p2 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addBinary(Binary val) {
//
//        }
//    };
//
//    PrimitiveConverter p3 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addBinary(Binary val) {
//
//        }
//    };
//
//    PrimitiveConverter p4 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addBinary(Binary val) {
//
//        }
//    };

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

    PrimitiveConverter p8 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxLongitude = value;
        }
    };

    PrimitiveConverter p9 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxLatitude = value;
        }
    };

    PrimitiveConverter p11 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            pivotsLongitude = val.getBytes();
        }
    };

    PrimitiveConverter p12 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            pivotsLatitude = val.getBytes();
        }
    };

    @Override
    public TrajectorySegmentWithPivotMetadata getCurrentRecord() {

        if(pivotsLongitude == null){
            return TrajectorySegmentWithPivotMetadata.newTrajectorySegmentWithPivotMetadata(new TrajectorySegment(objectId, null, minLongitude, minLatitude,maxLongitude, maxLatitude),null);
        }

        ByteBuffer pLongitude = ByteBuffer.wrap(pivotsLongitude);
        ByteBuffer pLatitude = ByteBuffer.wrap(pivotsLatitude);

        SpatialPoint[] spatialPoints = new SpatialPoint[pLongitude.array().length/8];
        for (int i = 0; i < spatialPoints.length; i++) {
            spatialPoints[i] = new SpatialPoint(pLongitude.getDouble(),pLatitude.getDouble());
        }

        pivotsLongitude = null;
        pivotsLatitude = null;
        return TrajectorySegmentWithPivotMetadata.newTrajectorySegmentWithPivotMetadata(new TrajectorySegment(objectId, null, minLongitude, minLatitude,maxLongitude, maxLatitude),spatialPoints);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
