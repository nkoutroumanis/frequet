package gr.ds.unipi.spatialnodb.messages.common;

import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.shapes.STPoint;
import org.davidmoten.hilbert.Ranges;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

public class IndexUtils implements Serializable {
    protected final double minLon;
    protected final double maxLon;
    protected final double minLat;
    protected final double maxLat;
    protected final long maxOrdinates;

    public IndexUtils(double minLon, double minLat, double maxLon, double maxLat, long maxOrdinates) {
        this.minLon = minLon;
        this.maxLon = maxLon;
        this.minLat = minLat;
        this.maxLat = maxLat;
        this.maxOrdinates = maxOrdinates;
    }

    public long[] scale(SpatialPoint stp){
        return scale(stp.getLongitude(), stp.getLatitude());
    }

    protected static long scale(double d, long max) {
        if (!(Double.compare(d, 0) != -1 && Double.compare(d, 1) != 1)) {
            throw new IllegalArgumentException();
        }

        if (d == 1) {
            return max;
        } else {
            return Math.round(Math.floor(d * (max + 1)));
        }
    }

    public long[] scale(double lon, double lat) {
        long x = scale((lon - minLon) / (maxLon - minLon), maxOrdinates);
        long y = scale((lat - minLat) / (maxLat - minLat), maxOrdinates);
        return new long[]{x, y};
    }

    public Optional<STPoint[]> clipping(long[] cube, SpatioTemporalPoint stp1, SpatioTemporalPoint stp2){
        return clipping(cube, stp1.getLongitude(), stp1.getLatitude(), stp1.getTimestamp(), stp2.getLongitude(), stp2.getLatitude(), stp2.getTimestamp());
    }

    public Optional<STPoint[]> clipping(long[] cube, double x1, double y1, long t1, double x2, double y2, long t2) {

        double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
        double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));

        double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
        double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));

        return HilbertUtil.liangBarskyTimeInterpolation(x1, y1, t1, x2, y2, t2, xMin, yMin, xMax, yMax);
    }
}
