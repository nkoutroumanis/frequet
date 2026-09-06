package tman.impl;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

public final class CachedShapeQuery {

    public interface SignatureCache {
        long[] signaturesForCell(long quadrantCode);
    }

    private final XZConfig cfg;
    private final XZTrajectoryIndex idx;
    private final SignatureCache cache;

    public CachedShapeQuery(XZConfig cfg, XZTrajectoryIndex idx, SignatureCache cache) {
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
        double extX1() { return x1 + (x1 - x0); }
        double extY1() { return y1 + (y1 - y0); }
        boolean containedIn(double[] q) {
            return q[0] <= x0 && q[1] <= y0 && q[2] >= extX1() && q[3] >= extY1();
        }
        boolean overlaps(double[] q) {
            return q[2] >= x0 && q[3] >= y0 && q[0] <= extX1() && q[1] <= extY1();
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

    private long queryMask(QuadElem cell, double[] q) {
        double cw = (cell.x1 - cell.x0) / cfg.beta;
        double chh = (cell.y1 - cell.y0) / cfg.alpha;
        long mask = 0L;
        for (int row = 0; row < cfg.alpha; row++) {
            double sy0 = cell.y0 + row * chh, sy1 = sy0 + chh;
            if (sy1 < q[1] || sy0 > q[3]) { continue; }
            for (int col = 0; col < cfg.beta; col++) {
                double sx0 = cell.x0 + col * cw, sx1 = sx0 + cw;
                if (sx1 < q[0] || sx0 > q[2]) { continue; }
                mask |= (1L << (row * cfg.beta + col));
            }
        }
        return mask;
    }

    public List<LongRange> queryRanges(double[] window, int maxWork) {
        double[] q = {cfg.normX(window[0]), cfg.normY(window[1]),
                      cfg.normX(window[2]), cfg.normY(window[3])};
        List<LongRange> ranges = new ArrayList<LongRange>();
        List<Long> exactKeys = new ArrayList<Long>();
        Deque<QuadElem> stack = new ArrayDeque<QuadElem>();
        stack.push(new QuadElem(new int[0], 0, 0, 1, 1));   // seed from ROOT

        int work = 0;
        while (!stack.isEmpty() && work++ < maxWork) {
            QuadElem e = stack.pop();
            if (e.containedIn(q)) {
                // whole subtree: no cache needed, emit one range
                long code = idx.quadrantCode(e.seq);
                long en = idx.enlargedNumber(e.seq.length);
                ranges.add(new LongRange(idx.tShape(code, 0), idx.tShape(code + en, 0)));
            } else if (e.overlaps(q)) {
                // partial: cache tells us which signatures actually exist here
                long code = idx.quadrantCode(e.seq);
                long[] present = cache.signaturesForCell(code);
                if (present != null && present.length > 0) {
                    long mask = queryMask(e, q);
                    for (long s : present) {
                        if ((s & mask) != 0L) { exactKeys.add(idx.tShape(code, s)); }
                    }
                }
                if (e.seq.length < cfg.g) {
                    for (QuadElem c : e.children()) { stack.push(c); }
                }
            }
        }

        // coalesce the exact partial-cell keys and merge with contained ranges
        long[] ek = new long[exactKeys.size()];
        for (int i = 0; i < ek.length; i++) { ek[i] = exactKeys.get(i); }
        java.util.Arrays.sort(ek);
        ranges.addAll(keysToRanges(ek));
        return mergeRanges(ranges);
    }

    public List<LongRange> queryRanges(double[] window) {
        return queryRanges(window, 200_000);
    }

    private static List<LongRange> mergeRanges(List<LongRange> raw) {
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

    public long[] queryKeys(double[] window, int maxWork) {
        double[] q = {cfg.normX(window[0]), cfg.normY(window[1]),
                      cfg.normX(window[2]), cfg.normY(window[3])};
        List<Long> keys = new ArrayList<Long>();
        Deque<QuadElem> stack = new ArrayDeque<QuadElem>();
        stack.push(new QuadElem(new int[0], 0, 0, 1, 1));   // seed from ROOT

        int work = 0;
        while (!stack.isEmpty() && work++ < maxWork) {
            QuadElem e = stack.pop();
            boolean contained = e.containedIn(q);
            boolean overlaps = contained || e.overlaps(q);
            if (!overlaps) { continue; }

            long code = idx.quadrantCode(e.seq);
            long[] present = cache.signaturesForCell(code);
            if (present != null && present.length > 0) {
                if (contained) {
                    for (long s : present) { keys.add(idx.tShape(code, s)); }
                } else {
                    long mask = queryMask(e, q);
                    for (long s : present) {
                        if ((s & mask) != 0L) { keys.add(idx.tShape(code, s)); }
                    }
                }
            }
            if (!contained && e.seq.length < cfg.g) {
                for (QuadElem c : e.children()) { stack.push(c); }
            }
            else if (contained && e.seq.length < cfg.g) {
                for (QuadElem c : e.children()) { stack.push(c); }
            }
        }

        long[] out = new long[keys.size()];
        for (int i = 0; i < out.length; i++) { out[i] = keys.get(i); }
        java.util.Arrays.sort(out);
        return out;
    }

    public long[] queryKeys(double[] window) {
        return queryKeys(window, 200_000);
    }

    /** Coalesce exact keys into minimal [lo,hi) ranges for range-scan APIs. */
    public static List<LongRange> keysToRanges(long[] sortedKeys) {
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
}
