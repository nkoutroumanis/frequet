package gr.ds.unipi.spatialnodb.messages.common.vre;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class VRERecord implements Serializable {

    private final String objectId;
    private final SpatialPoint[] spatialPoints;

    private final double minLongitude;
    private final double minLatitude;
    private final double maxLongitude;
    private final double maxLatitude;

    private final int serialNumber;
    private final int segmentType;

    private final SpatialPoint firstPoint;
    private final SpatialPoint lastPoint;

    private final byte[] signature;

    public VRERecord(String objectId, int serialNumber, int segmentType, SpatialPoint[] spatialPoints, double minLongitude, double minLatitude, double maxLongitude, double maxLatitude, SpatialPoint firstPoint, SpatialPoint lastPoint, byte[] signature) {
        this.objectId = objectId;
        this.serialNumber = serialNumber;
        this.segmentType = segmentType;
        this.spatialPoints = spatialPoints;
        this.minLongitude = minLongitude;
        this.minLatitude = minLatitude;
        this.maxLongitude = maxLongitude;
        this.maxLatitude = maxLatitude;
        this.firstPoint = firstPoint;
        this.lastPoint = lastPoint;
        this.signature = signature;
    }

    public VRERecord(String objectId, List<VRERecord> trajectorySegments) {

        if(trajectorySegments.size()!=1) {
            this.objectId = objectId;

            int spatialPointsNum = 0;

            for (int i = 0; i < trajectorySegments.size(); i++) {
                spatialPointsNum = spatialPointsNum + (trajectorySegments.get(i).getSpatialPoints().length);
            }

            this.spatialPoints = new SpatialPoint[spatialPointsNum];
            int arrayIndex = 0;

            for (int i = 0; i < trajectorySegments.size(); i++) {
                for (SpatialPoint spatialPoint : trajectorySegments.get(i).getSpatialPoints()) {
                    spatialPoints[arrayIndex++] = spatialPoint;
                }
            }
        }
        else{
            this.objectId = objectId;
            this.spatialPoints = trajectorySegments.get(0).getSpatialPoints();
        }

        this.serialNumber=1;
        this.segmentType=3;
        this.signature=null;

        this.minLongitude = -1;
        this.minLatitude = -1;
        this.maxLongitude = -1;
        this.maxLatitude = -1;
        this.firstPoint = spatialPoints[0];
        this.lastPoint = spatialPoints[spatialPoints.length-1];
    }

    public String getObjectId() {
        return objectId;
    }

    public double getMinLongitude() {
        return minLongitude;
    }

    public double getMinLatitude() {
        return minLatitude;
    }

    public double getMaxLongitude() {
        return maxLongitude;
    }

    public double getMaxLatitude() {
        return maxLatitude;
    }

    public SpatialPoint[] getSpatialPoints() {
        return spatialPoints;
    }

    @Override
    public String toString() {
        return "TrajectorySegment{" +
                "objectId='" + objectId + '\'' +
                ", spatioTemporalPoints=" + Arrays.toString(spatialPoints) +
                ", minLongitude=" + minLongitude +
                ", minLatitude=" + minLatitude +
                ", maxLongitude=" + maxLongitude +
                ", maxLatitude=" + maxLatitude +
                ", serialNumber=" + serialNumber +
                ", segmentType=" + segmentType +
                ", firstPoint=" + firstPoint +
                ", lastPoint=" + lastPoint +
                ", signature=" + Arrays.toString(signature) +
                '}';
    }

    public int getSerialNumber() {
        return serialNumber;
    }

    public int getSegmentType() {
        return segmentType;
    }

    public byte[] getSignature() {
        return signature;
    }

    public SpatialPoint getFirstPoint() {
        return firstPoint;
    }
    public SpatialPoint getLastPoint() {
        return lastPoint;
    }
}
