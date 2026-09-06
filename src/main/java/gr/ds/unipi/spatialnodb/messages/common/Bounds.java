package gr.ds.unipi.spatialnodb.messages.common;

import java.io.Serializable;
import java.util.List;

import gr.ds.unipi.spatialnodb.messages.common.tman.TManRecord;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegment;
import scala.Tuple3;

public class Bounds implements Serializable {

    private double minLon = Double.MAX_VALUE;
    private double minLat = Double.MAX_VALUE;
    private double maxLon = -Double.MAX_VALUE;
    private double maxLat = -Double.MAX_VALUE;

    public void add(List<Tuple3<Double, Double, Long>> stPoints) {

//        double minLongitude = Double.MAX_VALUE;
//        double minLatitude = Double.MAX_VALUE;
//        long minTimestamp = Long.MAX_VALUE;
//
//        double maxLongitude = -Double.MAX_VALUE;
//        double maxLatitude = -Double.MAX_VALUE;
//        long maxTimestamp = Long.MIN_VALUE;

        for (Tuple3<Double, Double, Long> stP : stPoints) {
            if (Double.compare(minLon, stP._1()) == 1) {
                minLon = stP._1();
            }
            if (Double.compare(minLat, stP._2()) == 1) {
                minLat = stP._2();
            }
            if (Double.compare(maxLon, stP._1()) == -1) {
                maxLon = stP._1();
            }
            if (Double.compare(maxLat, stP._2()) == -1) {
                maxLat = stP._2();
            }
        }
    }

    public void add(TrajectorySegment ts) {
        minLon = Math.min(minLon, ts.getMinLongitude());
        minLat = Math.min(minLat, ts.getMinLatitude());
        maxLon = Math.max(maxLon, ts.getMaxLongitude());
        maxLat = Math.max(maxLat, ts.getMaxLatitude());
    }

    public void add(double[] bounds) {
        minLon = Math.min(minLon, bounds[0]);
        minLat = Math.min(minLat, bounds[1]);
        maxLon = Math.max(maxLon, bounds[2]);
        maxLat = Math.max(maxLat, bounds[3]);
    }


    public void merge(Bounds o) {
        minLon = Math.min(minLon, o.minLon);
        minLat = Math.min(minLat, o.minLat);
        maxLon = Math.max(maxLon, o.maxLon);
        maxLat = Math.max(maxLat, o.maxLat);
    }

    public double getMinLongitude() {
        return minLon;
    }
    public double getMinLatitude() {
        return minLat;
    }
    public double getMaxLongitude() {
        return maxLon;
    }
    public double getMaxLatitude() {
        return maxLat;
    }

}