package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

//import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
//import fi.iki.yak.ts.compression.gorilla.Decompressor;
//import fi.iki.yak.ts.compression.gorilla.Value;
//import gr.aueb.delorean.chimp.ChimpNDecompressor;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import gr.ds.unipi.spatialnodb.messages.common.SpatioTemporalPoint;
import org.apache.parquet.io.api.*;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class TrajectorySegmentWithIntervalMetadataMaterializer extends RecordMaterializer<TrajectorySegmentWithIntervalMetadata> {

    private String objectId;

    private byte[] longitude;
    private byte[] latitude;

    private double minLongitude;
    private double minLatitude;
    private double maxLongitude;
    private double maxLatitude;

    private long intervalStart;
    private long intervalStop;

    GroupConverter groupConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i==0){
                return p0;
            } else if(i==1){
                return p2;
            } else if(i==2){
                return p3;
            }else if(i==3){
                return p5;
            } else if(i==4){
                return p6;
            } else if(i==5){
                return p8;
            } else if(i==6){
                return p9;
            } else if(i==7){
                return p11;
            } else if(i==8){
                return p12;
            }
            return null;
        }

        @Override
        public void start() {
            intervalStart=0;
            intervalStop=0;
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

    PrimitiveConverter p2 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            longitude = val.getBytes();
        }
    };

    PrimitiveConverter p3 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            latitude = val.getBytes();
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
        public void addLong(long value) {
            intervalStart = value;
        }
    };

    PrimitiveConverter p12 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            intervalStop = value;
        }
    };

    @Override
    public TrajectorySegmentWithIntervalMetadata getCurrentRecord() {
//        if(objectId.equals("538002828")&& segment==2){
//            System.out.println("Here1 "+ intervalStart);
//        }

        ByteBuffer bLongitude = ByteBuffer.wrap(longitude);
        ByteBuffer bLatitude = ByteBuffer.wrap(latitude);

        SpatialPoint[] spatialPoints = new SpatialPoint[bLongitude.array().length/8];
        for (int i = 0; i < spatialPoints.length; i++) {
            spatialPoints[i] = new SpatialPoint(bLongitude.getDouble(),bLatitude.getDouble());
        }

        long[] intervals = null;
        if(intervalStart!= 0L){
            intervals = new long[]{intervalStart, intervalStop};
        }

//        if(objectId.equals("538002828")&& segment==2){
//            System.out.println("Here2 "+ Arrays.toString(intervals));
//        }
        return TrajectorySegmentWithIntervalMetadata.newTrajectorySegmentWithIntervalMetadata(new TrajectorySegment(objectId, spatialPoints, minLongitude, minLatitude,maxLongitude, maxLatitude), intervals);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
