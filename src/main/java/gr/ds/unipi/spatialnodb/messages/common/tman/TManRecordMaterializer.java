package gr.ds.unipi.spatialnodb.messages.common.tman;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import org.apache.parquet.io.api.*;
import tman.impl.DPFeatureExtractor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TManRecordMaterializer extends RecordMaterializer<TManRecord> {

    private String objectId;
    private byte[] longitude;
    private byte[] latitude;
    private List<Integer> dpPointIndexes;
    private byte[] dpMbrs;
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
                return p3;
            } else if(i==4){
                return p4;
            } else if(i==5){
                return p5;
            }
            return null;
        }

        @Override
        public void start() {
            dpPointIndexes = new ArrayList<>();
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
        public void addBinary(Binary val) {
            longitude = val.getBytes();
        }
    };

    PrimitiveConverter p2 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            latitude = val.getBytes();
        }
    };

    PrimitiveConverter p3 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long val) {

        }

    };

    PrimitiveConverter p4 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addInt(int val) {
            dpPointIndexes.add(val);
        }
    };

    PrimitiveConverter p5 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            dpMbrs = val.getBytes();
        }
    };
    @Override
    public TManRecord getCurrentRecord() {

        ByteBuffer bLongitude = ByteBuffer.wrap(longitude);
        ByteBuffer bLatitude = ByteBuffer.wrap(latitude);

        SpatialPoint[] spatialPoints = new SpatialPoint[bLongitude.array().length/8];
        for (int i = 0; i < spatialPoints.length; i++) {
            spatialPoints[i] = new SpatialPoint(bLongitude.getDouble(),bLatitude.getDouble());
        }

        int[] dpPointsIndexes = new int[this.dpPointIndexes.size()];
        for (int i = 0; i < this.dpPointIndexes.size(); i++) {
            dpPointsIndexes[i]= this.dpPointIndexes.get(i);
        }
        this.dpPointIndexes.clear();

        DPFeatureExtractor.BoundingBox[] dpMbrs;
        try (ObjectInputStream ois =
                     new ObjectInputStream(new ByteArrayInputStream(this.dpMbrs))) {
            dpMbrs = (DPFeatureExtractor.BoundingBox[]) ois.readObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return new TManRecord(objectId, spatialPoints, dpPointsIndexes, dpMbrs);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
