package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import gr.ds.unipi.spatialnodb.messages.common.SpatioTemporalPoint;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class TrajectorySegment implements Serializable {

    private final String objectId;
    private final SpatialPoint[] spatialPoints;

    private final double minLongitude;
    private final double minLatitude;
    private final double maxLongitude;
    private final double maxLatitude;

//    public TrajectorySegment(String objectId, long trajectoryId, long segment, SpatioTemporalPoint[] spatioTemporalPoints, double minLongitude, double minLatitude, long minTimestamp, double maxLongitude, double maxLatitude, long maxTimestamp) {
//        this.objectId = objectId;
//        this.trajectoryId = trajectoryId;
//        this.segment = segment;
//        this.spatioTemporalPoints = spatioTemporalPoints;
//        this.minLongitude = minLongitude;
//        this.minLatitude = minLatitude;
//        this.minTimestamp = minTimestamp;
//        this.maxLongitude = maxLongitude;
//        this.maxLatitude = maxLatitude;
//        this.maxTimestamp = maxTimestamp;
//    }

    public TrajectorySegment(String objectId, SpatialPoint[] spatialPoints, double minLongitude, double minLatitude, double maxLongitude, double maxLatitude) {
        this.objectId = objectId;
        this.spatialPoints = spatialPoints;
        this.minLongitude = minLongitude;
        this.minLatitude = minLatitude;
        this.maxLongitude = maxLongitude;
        this.maxLatitude = maxLatitude;
    }

    public TrajectorySegment(String objectId, List<TrajectorySegment> trajectorySegments) {

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

        this.minLongitude = -1;
        this.minLatitude = -1;
        this.maxLongitude = -1;
        this.maxLatitude = -1;
    }

    public String getObjectId() {
        return objectId;
    }

//    public long getTrajectoryId() {
//        return trajectoryId;
//    }

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
                '}';
    }
}
