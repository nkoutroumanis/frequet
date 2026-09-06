package tman.impl;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

public final class FrechetRangeQuery {

    private final XZConfig cfg;
    private final XZTrajectoryIndex idx;

    public FrechetRangeQuery(XZConfig cfg, XZTrajectoryIndex idx) {
        this.cfg = cfg;
        this.idx = idx;
    }

    private static final class QuadElem {
        final int[] seq;
        final double x0, y0, x1, y1;   // normalized cell bounds
        QuadElem(int[] seq, double x0, double y0, double x1, double y1) {
            this.seq = seq; this.x0 = x0; this.y0 = y0; this.x1 = x1; this.y1 = y1;
        }
        QuadElem[] children() {
            double mx = (x0 + x1) / 2.0, my = (y0 + y1) / 2.0;
            return new QuadElem[]{
                new QuadElem(app(seq,0), x0, y0, mx, my),
                new QuadElem(app(seq,1), mx, y0, x1, my),
                new QuadElem(app(seq,2), x0, my, mx, y1),
                new QuadElem(app(seq,3), mx, my, x1, y1)
            };
        }
        private static int[] app(int[] a, int v) {
            int[] b = new int[a.length + 1];
            System.arraycopy(a, 0, b, 0, a.length); b[a.length] = v; return b;
        }
    }

    /** Distance from point (px,py) to axis-aligned rect [x0,x1]x[y0,y1]; 0 if inside. */
    private static double pointRectDist(double px, double py,
                                        double x0, double y0, double x1, double y1) {
        double dx = px < x0 ? x0 - px : (px > x1 ? px - x1 : 0.0);
        double dy = py < y0 ? y0 - py : (py > y1 ? py - y1 : 0.0);
        return Math.sqrt(dx * dx + dy * dy);
    }

    /** MINDIST(cell, Q) = min over query points of point-to-cell distance. */
    private double minDistToQuery(QuadElem e, double[][] qNorm) {
        double best = Double.POSITIVE_INFINITY;
        for (double[] p : qNorm) {
            double d = pointRectDist(p[0], p[1], e.x0, e.y0, e.x1, e.y1);
            if (d < best) { best = d; }
            if (best == 0.0) { break; }
        }
        return best;
    }

    /**
     * Candidate ranges for a discrete-Fréchet threshold query.
     *
     * @param queryPoints Q's vertices [[x,y],...] in USER coordinates
     * @param theta       Fréchet threshold in USER distance units
     * @param maxWork     traversal cap
     * @return merged, ascending [lo,hi) candidate ranges
     */
    public List<LongRange> queryRanges(double[][] queryPoints, double theta, int maxWork) {
        // normalize query points and theta into [0,1] space.
        // NOTE assumes an (approximately) square, uniformly-scaled domain so a
        // single scalar theta is meaningful; see caveat in the response.
        double sx = cfg.xMax - cfg.xMin;
        double sy = cfg.yMax - cfg.yMin;
        double[][] qn = new double[queryPoints.length][2];
        for (int i = 0; i < queryPoints.length; i++) {
            qn[i][0] = cfg.normX(queryPoints[i][0]);
            qn[i][1] = cfg.normY(queryPoints[i][1]);
        }
        // normalized theta on each axis; use the larger so we never under-prune
        double thNorm = theta / Math.min(sx, sy);

        List<LongRange> raw = new ArrayList<LongRange>();
        Deque<QuadElem> stack = new ArrayDeque<QuadElem>();
        stack.push(new QuadElem(new int[0], 0, 0, 1, 1));

        int work = 0;
        while (!stack.isEmpty() && work++ < maxWork) {
            QuadElem e = stack.pop();

            // PRUNE: cell too far from the whole query polyline -> drop subtree
            if (minDistToQuery(e, qn) > thNorm) {
                continue;
            }

            // this cell is a candidate: emit its OWN block (one cell's worth)
            long code = idx.quadrantCode(e.seq);
            raw.add(new LongRange(idx.tShape(code, 0), idx.tShape(code + 1, 0)));

            // recurse: descendants may or may not survive the same MINDIST test
            if (e.seq.length < cfg.g) {
                for (QuadElem c : e.children()) { stack.push(c); }
            }
        }
        return merge(raw);
    }

    public List<LongRange> queryRanges(double[][] queryPoints, double theta) {
        return queryRanges(queryPoints, theta, 500_000);
    }

    private static List<LongRange> merge(List<LongRange> raw) {
        List<LongRange> out = new ArrayList<LongRange>();
        if (raw.isEmpty()) { return out; }
        Collections.sort(raw, new Comparator<LongRange>() {
            public int compare(LongRange a, LongRange b) {
                int c = Long.compare(a.lo, b.lo);
                return c != 0 ? c : Long.compare(a.hi, b.hi);
            }
        });
        long lo = raw.get(0).lo, hi = raw.get(0).hi;
        for (int i = 1; i < raw.size(); i++) {
            LongRange r = raw.get(i);
            if (r.lo <= hi) { hi = Math.max(hi, r.hi); }
            else { out.add(new LongRange(lo, hi)); lo = r.lo; hi = r.hi; }
        }
        out.add(new LongRange(lo, hi));
        return out;
    }
}
