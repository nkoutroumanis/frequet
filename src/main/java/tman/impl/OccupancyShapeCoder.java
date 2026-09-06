package tman.impl;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;

public final class OccupancyShapeCoder {

    private OccupancyShapeCoder() { }

    /** Normalized [0,1] bounds {x0,y0,x1,y1} of the cell for a quadrant sequence. */
    static double[] cellBounds(int[] seq) {
        double x0 = 0, y0 = 0, x1 = 1, y1 = 1;
        for (int q : seq) {
            double mx = (x0 + x1) / 2.0;
            double my = (y0 + y1) / 2.0;
            boolean right = (q & 1) == 1;
            boolean top = (q & 2) == 2;
            if (right) { x0 = mx; } else { x1 = mx; }
            if (top)   { y0 = my; } else { y1 = my; }
        }
        return new double[]{x0, y0, x1, y1};
    }

    private static int bitIndex(int row, int col, XZConfig cfg) {
        return row * cfg.beta + col;
    }

    private static int clamp(int v, int lo, int hi) {
        return v < lo ? lo : (v > hi ? hi : v);
    }

    /**
     * Point-based occupancy shape code for a trajectory.
     *
     * @param points  GPS vertices [[x,y],...] in USER coordinates
     * @param cellSeq home cell quadrant sequence
     * @param cfg     config (alpha rows, beta cols)
     * @return alpha*beta-bit shape code in [0, 2^(alpha*beta))
     */
    public static long shapeCodeForTrajectory(SpatialPoint[] points, int[] cellSeq,
                                              XZConfig cfg) {
        double[] cb = cellBounds(cellSeq);
        double cx0 = cb[0], cy0 = cb[1], cw = cb[2] - cb[0], ch = cb[3] - cb[1];
        if (cw <= 0 || ch <= 0 || points.length == 0) {
            return 0L;
        }
        long bits = 0L;
        for (SpatialPoint p : points) {
            double fx = (cfg.normX(p.getLongitude()) - cx0) / cw;   // fractional pos in cell
            double fy = (cfg.normY(p.getLatitude()) - cy0) / ch;
            if (fx < 0 || fx > 1 || fy < 0 || fy > 1) {
                continue;                                // point outside home cell
            }
            int col = clamp((int) (fx * cfg.beta),  0, cfg.beta  - 1);
            int row = clamp((int) (fy * cfg.alpha), 0, cfg.alpha - 1);
            bits |= (1L << bitIndex(row, col, cfg));
        }
        return bits;
    }

    /** Bounding box {xmin,ymin,xmax,ymax} of a polyline, for the home-cell step. */
    public static double[] bbox(double[][] points) {
        double xmin = Double.POSITIVE_INFINITY, ymin = Double.POSITIVE_INFINITY;
        double xmax = Double.NEGATIVE_INFINITY, ymax = Double.NEGATIVE_INFINITY;
        for (double[] p : points) {
            if (p[0] < xmin) { xmin = p[0]; }
            if (p[1] < ymin) { ymin = p[1]; }
            if (p[0] > xmax) { xmax = p[0]; }
            if (p[1] > ymax) { ymax = p[1]; }
        }
        return new double[]{xmin, ymin, xmax, ymax};
    }

    /**
     * Full index for a trajectory: home cell -> quadrant code -> pack with the
     * point-based occupancy shape code.
     */
    public static long indexTrajectory(SpatialPoint[] points, XZTrajectoryIndex idx, double[] bbox, XZConfig cfg) {
        int[] seq = idx.homeCellSequence(bbox);
        long code = idx.quadrantCode(seq);
        long s = shapeCodeForTrajectory(points, seq, cfg);
        return idx.tShape(code, s);
    }
}
