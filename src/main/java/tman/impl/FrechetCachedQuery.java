package tman.impl;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

public final class FrechetCachedQuery {

    public interface SignatureCache {
        long[] signaturesForCell(long quadrantCode);
    }

    private final XZConfig cfg;
    private final XZTrajectoryIndex idx;
    private final SignatureCache cache;

    public FrechetCachedQuery(XZConfig cfg, XZTrajectoryIndex idx, SignatureCache cache) {
        this.cfg = cfg;
        this.idx = idx;
        this.cache = cache;
    }

    private static final class QuadElem {
        final int[] seq;
        final double x0, y0, x1, y1;
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

    private static double pointRectDist(double px, double py,
                                        double x0, double y0, double x1, double y1) {
        double dx = px < x0 ? x0 - px : (px > x1 ? px - x1 : 0.0);
        double dy = py < y0 ? y0 - py : (py > y1 ? py - y1 : 0.0);
        return Math.sqrt(dx * dx + dy * dy);
    }

    private double minDistToQuery(double x0, double y0, double x1, double y1,
                                  double[][] qNorm) {
        double best = Double.POSITIVE_INFINITY;
        for (double[] p : qNorm) {
            double d = pointRectDist(p[0], p[1], x0, y0, x1, y1);
            if (d < best) { best = d; }
            if (best == 0.0) { break; }
        }
        return best;
    }

    /**
     * @param queryPoints Q's vertices in USER coordinates
     * @param theta       threshold in USER distance units
     */
    public List<LongRange> queryRanges(SpatialPoint[] spatialPoint, double theta, int maxWork) {
        double sx = cfg.xMax - cfg.xMin, sy = cfg.yMax - cfg.yMin;
        double[][] qn = new double[spatialPoint.length][2];
        for (int i = 0; i < spatialPoint.length; i++) {
            qn[i][0] = cfg.normX(spatialPoint[i].getLongitude());
            qn[i][1] = cfg.normY(spatialPoint[i].getLatitude());
        }

        double thNorm = theta / Math.min(sx, sy);   // conservative (never under-prunes)

        List<LongRange> ranges = new ArrayList<LongRange>();  // whole-block ranges
        List<Long> exactKeys = new ArrayList<Long>();         // filtered signatures
        Deque<QuadElem> stack = new ArrayDeque<QuadElem>();
        stack.push(new QuadElem(new int[0], 0, 0, 1, 1));

        int work = 0;
        while (!stack.isEmpty() && work++ < maxWork) {
            QuadElem e = stack.pop();

            // STAGE 1: cell-level MINDIST prune (drops subtree)
            if (minDistToQuery(e.x0, e.y0, e.x1, e.y1, qn) > thNorm) {
                continue;
            }

            long code = idx.quadrantCode(e.seq);
            long[] present = cache.signaturesForCell(code);

            if (present != null && present.length > 0) {
                // which sub-cells are occupied (union of all present signatures)?
                long occupied = 0L;
                for (long s : present) { occupied |= s; }

                // STAGE 2: per occupied sub-cell, compute MINDIST; build surviving mask
                long surviveMask = subCellSurviveMask(e, occupied, qn, thNorm);

                if (surviveMask == occupied) {
                    // every occupied sub-cell within theta -> nothing filtered
                    ranges.add(new LongRange(idx.tShape(code, 0), idx.tShape(code + 1, 0)));
                } else {
                    // keep signatures touching at least one surviving sub-cell
                    for (long s : present) {
                        if ((s & surviveMask) != 0L) {
                            exactKeys.add(idx.tShape(code, s));
                        }
                    }
                }
            }
            // else: no cached signatures for this cell -> nothing to emit here
            // (if a cell can hold data without a cache entry, emit its block instead)

            if (e.seq.length < cfg.g) {
                for (QuadElem c : e.children()) { stack.push(c); }
            }
        }

        long[] ek = new long[exactKeys.size()];
        for (int i = 0; i < ek.length; i++) { ek[i] = exactKeys.get(i); }
        java.util.Arrays.sort(ek);
        ranges.addAll(keysToRanges(ek));
        return merge(ranges);
    }

    public List<LongRange> queryRanges(SpatialPoint[] queryPoints, double theta) {
        return queryRanges(queryPoints, theta, 500_000);
    }

    /** For each occupied sub-cell, keep it iff MINDIST(sub-cell, Q) <= thNorm. */
    private long subCellSurviveMask(QuadElem e, long occupied,
                                    double[][] qn, double thNorm) {
        double cw = (e.x1 - e.x0) / cfg.beta;
        double chh = (e.y1 - e.y0) / cfg.alpha;
        long survive = 0L;
        long bits = occupied;
        while (bits != 0L) {
            int b = Long.numberOfTrailingZeros(bits);
            bits &= (bits - 1);                       // clear lowest set bit
            int row = b / cfg.beta;
            int col = b % cfg.beta;
            double sx0 = e.x0 + col * cw, sy0 = e.y0 + row * chh;
            double d = minDistToQuery(sx0, sy0, sx0 + cw, sy0 + chh, qn);
            if (d <= thNorm) { survive |= (1L << b); }
        }
        return survive;
    }

    private static List<LongRange> keysToRanges(long[] sortedKeys) {
        List<LongRange> out = new ArrayList<LongRange>();
        if (sortedKeys.length == 0) { return out; }
        long lo = sortedKeys[0], prev = sortedKeys[0];
        for (int i = 1; i < sortedKeys.length; i++) {
            if (sortedKeys[i] == prev + 1) { prev = sortedKeys[i]; }
            else { out.add(new LongRange(lo, prev + 1)); lo = sortedKeys[i]; prev = sortedKeys[i]; }
        }
        out.add(new LongRange(lo, prev + 1));
        return out;
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
