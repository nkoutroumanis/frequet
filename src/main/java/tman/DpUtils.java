package tman;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import tman.impl.DPFeatureExtractor;

public final class DpUtils {

    public static double distanceToNearestBox(SpatialPoint p, DPFeatureExtractor.BoundingBox[] boxes) {
        double min = Double.POSITIVE_INFINITY;
        for (DPFeatureExtractor.BoundingBox box : boxes) {
            min = Math.min(min, distancePointToBox(p, box));
            if (min == 0.0) return 0.0;
        }
        return min;
    }

    /** d(bbox, T.B) = max over bbox's 4 edges of [ min over T.B of edge-to-box distance ] */
    public static double boxToBoxSetDistance(DPFeatureExtractor.BoundingBox box,
                                              DPFeatureExtractor.BoundingBox[] targetSet) {
        SpatialPoint[] c = box.getCorners();
        double maxOverEdges = 0.0;
        for (int i = 0; i < 4; i++) {
            SpatialPoint a = c[i];
            SpatialPoint b = c[(i + 1) % 4];
            double minOverTargets = Double.POSITIVE_INFINITY;
            for (DPFeatureExtractor.BoundingBox target : targetSet) {
                minOverTargets = Math.min(minOverTargets, distanceSegmentToBox(a, b, target));
                if (minOverTargets == 0.0) break;
            }
            maxOverEdges = Math.max(maxOverEdges, minOverTargets);
        }
        return maxOverEdges;
    }

    /** Point-to-polygon distance: 0 if inside/on the boundary, else min distance to the 4 edges. */
    private static double distancePointToBox(SpatialPoint p, DPFeatureExtractor.BoundingBox box) {
        SpatialPoint[] c = box.getCorners();
        if (pointInPolygon(p, c)) return 0.0;
        double min = Double.POSITIVE_INFINITY;
        for (int i = 0; i < 4; i++) {
            min = Math.min(min, distancePointToSegment(p, c[i], c[(i + 1) % 4]));
        }
        return min;
    }

    /** Segment-to-polygon distance: 0 if the segment touches/crosses/sits inside the polygon,
     *  else the min distance between the segment and each polygon edge. */
    private static double distanceSegmentToBox(SpatialPoint a, SpatialPoint b, DPFeatureExtractor.BoundingBox box) {
        SpatialPoint[] c = box.getCorners();
        if (pointInPolygon(a, c) || pointInPolygon(b, c)) return 0.0;
        double min = Double.POSITIVE_INFINITY;
        for (int i = 0; i < 4; i++) {
            min = Math.min(min, distanceSegmentToSegment(a, b, c[i], c[(i + 1) % 4]));
            if (min == 0.0) return 0.0;
        }
        return min;
    }

    private static boolean pointInPolygon(SpatialPoint p, SpatialPoint[] poly) {
        boolean inside = false;
        int n = poly.length;
        double px = p.getLongitude(), py = p.getLatitude();
        for (int i = 0, j = n - 1; i < n; j = i++) {
            double xi = poly[i].getLongitude(), yi = poly[i].getLatitude();
            double xj = poly[j].getLongitude(), yj = poly[j].getLatitude();
            boolean crosses = ((yi > py) != (yj > py))
                    && (px < (xj - xi) * (py - yi) / (yj - yi) + xi);
            if (crosses) inside = !inside;
        }
        return inside;
    }

    private static double distancePointToSegment(SpatialPoint p, SpatialPoint a, SpatialPoint b) {
        double px = p.getLongitude(), py = p.getLatitude();
        double ax = a.getLongitude(), ay = a.getLatitude();
        double bx = b.getLongitude(), by = b.getLatitude();
        double dx = bx - ax, dy = by - ay;
        double len2 = dx * dx + dy * dy;
        if (len2 == 0.0) return dist(p, a);
        double t = ((px - ax) * dx + (py - ay) * dy) / len2;
        t = Math.max(0.0, Math.min(1.0, t));
        double cx = ax + t * dx, cy = ay + t * dy;
        double ddx = px - cx, ddy = py - cy;
        return Math.sqrt(ddx * ddx + ddy * ddy);
    }

    private static double distanceSegmentToSegment(SpatialPoint a1, SpatialPoint a2, SpatialPoint b1, SpatialPoint b2) {
        if (segmentsIntersect(a1, a2, b1, b2)) return 0.0;
        return Math.min(
                Math.min(distancePointToSegment(a1, b1, b2), distancePointToSegment(a2, b1, b2)),
                Math.min(distancePointToSegment(b1, a1, a2), distancePointToSegment(b2, a1, a2))
        );
    }

    private static boolean segmentsIntersect(SpatialPoint p1, SpatialPoint p2, SpatialPoint p3, SpatialPoint p4) {
        double d1 = cross(p3, p4, p1);
        double d2 = cross(p3, p4, p2);
        double d3 = cross(p1, p2, p3);
        double d4 = cross(p1, p2, p4);

        if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0))
                && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) {
            return true;
        }
        if (d1 == 0 && onSegment(p3, p4, p1)) return true;
        if (d2 == 0 && onSegment(p3, p4, p2)) return true;
        if (d3 == 0 && onSegment(p1, p2, p3)) return true;
        if (d4 == 0 && onSegment(p1, p2, p4)) return true;
        return false;
    }

    private static double cross(SpatialPoint a, SpatialPoint b, SpatialPoint c) {
        return (b.getLongitude() - a.getLongitude()) * (c.getLatitude() - a.getLatitude())
                - (b.getLatitude() - a.getLatitude()) * (c.getLongitude() - a.getLongitude());
    }

    private static boolean onSegment(SpatialPoint a, SpatialPoint b, SpatialPoint p) {
        return Math.min(a.getLongitude(), b.getLongitude()) <= p.getLongitude()
                && p.getLongitude() <= Math.max(a.getLongitude(), b.getLongitude())
                && Math.min(a.getLatitude(), b.getLatitude()) <= p.getLatitude()
                && p.getLatitude() <= Math.max(a.getLatitude(), b.getLatitude());
    }

    private static double dist(SpatialPoint a, SpatialPoint b) {
        double dx = a.getLongitude() - b.getLongitude();
        double dy = a.getLatitude() - b.getLatitude();
        return Math.sqrt(dx * dx + dy * dy);
    }

}
