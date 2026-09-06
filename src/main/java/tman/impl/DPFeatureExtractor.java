package tman.impl;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public final class DPFeatureExtractor {

    private DPFeatureExtractor() {
    }

    public static final class BoundingBox implements Serializable {
        private final SpatialPoint[] corners;

        BoundingBox(SpatialPoint[] corners) {
            if (corners == null || corners.length != 4) {
                throw new IllegalArgumentException("A bounding box needs exactly 4 corners");
            }
            this.corners = corners;
        }

        public SpatialPoint[] getCorners() {
            return corners;
        }

        public SpatialPoint getCorner(int i) {
            return corners[i];
        }

        public String toWkt() {
            StringBuilder sb = new StringBuilder("POLYGON((");
            for (SpatialPoint c : corners) {
                sb.append(c.getLongitude()).append(' ').append(c.getLatitude()).append(", ");
            }
            sb.append(corners[0].getLongitude()).append(' ').append(corners[0].getLatitude());
            sb.append("))");
            return sb.toString();
        }

        @Override
        public String toString() {
            return toWkt();
        }
    }

    /** Result of DP-feature extraction for a single trajectory. */
    public static final class DPFeatures implements Serializable {
        private final int[] pointIndexes;
        private final BoundingBox[] boundingBoxes;

        DPFeatures(int[] pointIndexes, BoundingBox[] boundingBoxes) {
            this.pointIndexes = pointIndexes;
            this.boundingBoxes = boundingBoxes;
        }

        public int[] getPointIndexes() {
            return pointIndexes;
        }

        public BoundingBox[] getBoundingBoxes() {
            return boundingBoxes;
        }
    }

    /** Same as extract(trajectory, 0.01) -- 0.01 is the paper's default precision. */
    public static DPFeatures extract(SpatialPoint[] trajectory) {
        return extract(trajectory, 0.01);
    }

    /**
     * @param trajectory raw ordered trajectory points, length &gt;= 2
     * @param precision  DP perpendicular-distance threshold (theta)
     */
    public static DPFeatures extract(SpatialPoint[] trajectory, double precision) {
        if (trajectory == null || trajectory.length < 2) {
            throw new IllegalArgumentException("trajectory must contain at least 2 points");
        }
        if (precision <= 0) {
            throw new IllegalArgumentException("precision must be > 0");
        }

        int[] pointIndexes = douglasPeucker(trajectory, precision);

        BoundingBox[] boxes = new BoundingBox[pointIndexes.length - 1];
        for (int i = 0; i < pointIndexes.length - 1; i++) {
            boxes[i] = segmentAlignedBox(trajectory, pointIndexes[i], pointIndexes[i + 1]);
        }
        return new DPFeatures(pointIndexes, boxes);
    }

    // ---- Douglas-Peucker (recursive, standard) -----------------------------

    private static int[] douglasPeucker(SpatialPoint[] pts, double precision) {
        List<Integer> keep = new ArrayList<>();
        keep.add(0);
        simplify(pts, 0, pts.length - 1, precision, keep);
        keep.add(pts.length - 1);

        int[] result = new int[keep.size()];
        for (int i = 0; i < keep.size(); i++) {
            result[i] = keep.get(i);
        }
        return result;
    }

    private static void simplify(SpatialPoint[] pts, int start, int end, double precision, List<Integer> keep) {
        if (end <= start + 1) {
            return; // no interior points
        }
        double maxDist = -1.0;
        int maxIdx = -1;
        for (int i = start + 1; i < end; i++) {
            double d = perpendicularDistance(pts[i], pts[start], pts[end]);
            if (d > maxDist) {
                maxDist = d;
                maxIdx = i;
            }
        }
        if (maxDist > precision) {
            simplify(pts, start, maxIdx, precision, keep);
            keep.add(maxIdx);
            simplify(pts, maxIdx, end, precision, keep);
        }
        // else: all interior points within precision of segment -> dropped
    }

    // ---- Corrected segment-aligned bounding box ----------------------------
    //
    // Covers EVERY point in [startIdx, endIdx] by construction.

    private static BoundingBox segmentAlignedBox(SpatialPoint[] pts, int startIdx, int endIdx) {
        SpatialPoint a = pts[startIdx];
        SpatialPoint b = pts[endIdx];

        double dx = b.getLongitude() - a.getLongitude();
        double dy = b.getLatitude() - a.getLatitude();
        double len = Math.sqrt(dx * dx + dy * dy);

        // Local frame: U along the segment, V perpendicular. Fall back to the
        // coordinate axes if the segment has zero length (sp == ep).
        double ux, uy, vx, vy;
        if (len == 0.0) {
            ux = 1.0; uy = 0.0;
            vx = 0.0; vy = 1.0;
        } else {
            ux = dx / len; uy = dy / len;
            vx = -uy;      vy = ux;
        }

        double minU = Double.POSITIVE_INFINITY, maxU = Double.NEGATIVE_INFINITY;
        double minV = Double.POSITIVE_INFINITY, maxV = Double.NEGATIVE_INFINITY;

        // min/max over ALL points in range -> box is guaranteed to contain them all.
        for (int i = startIdx; i <= endIdx; i++) {
            double rx = pts[i].getLongitude() - a.getLongitude();
            double ry = pts[i].getLatitude() - a.getLatitude();
            double u = rx * ux + ry * uy;
            double v = rx * vx + ry * vy;
            if (u < minU) minU = u;
            if (u > maxU) maxU = u;
            if (v < minV) minV = v;
            if (v > maxV) maxV = v;
        }

        double[][] local = {
                {minU, minV}, {maxU, minV}, {maxU, maxV}, {minU, maxV}
        };
        SpatialPoint[] corners = new SpatialPoint[4];
        for (int i = 0; i < 4; i++) {
            double u = local[i][0];
            double v = local[i][1];
            double x = a.getLongitude() + u * ux + v * vx;
            double y = a.getLatitude()  + u * uy + v * vy;
            corners[i] = new SpatialPoint(x, y);
        }
        return new BoundingBox(corners);
    }

    // ---- geometry helpers --------------------------------------------------

    private static double perpendicularDistance(SpatialPoint p, SpatialPoint a, SpatialPoint b) {
        double dx = b.getLongitude() - a.getLongitude();
        double dy = b.getLatitude() - a.getLatitude();
        if (dx == 0 && dy == 0) {
            double ddx = p.getLongitude() - a.getLongitude();
            double ddy = p.getLatitude() - a.getLatitude();
            return Math.sqrt(ddx * ddx + ddy * ddy);
        }
        double num = Math.abs(dy * p.getLongitude() - dx * p.getLatitude()
                + b.getLongitude() * a.getLatitude() - b.getLatitude() * a.getLongitude());
        double den = Math.sqrt(dx * dx + dy * dy);
        return num / den;
    }
}