package tman.impl;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

public final class ShapeNarrowedQuery {

    private final XZConfig cfg;
    private final XZTrajectoryIndex idx;

    public ShapeNarrowedQuery(XZConfig cfg, XZTrajectoryIndex idx) {
        this.cfg = cfg;
        this.idx = idx;
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

    /**
     * queryMask: bit set for every alpha x beta sub-cell of `cell` that
     * intersects the (normalized) query window q = {qx0,qy0,qx1,qy1}.
     * Conservative: a sub-cell touching the window at all is included.
     */
    private long queryMask(QuadElem cell, double[] q) {
        double cw = (cell.x1 - cell.x0) / cfg.beta;    // sub-cell width
        double chh = (cell.y1 - cell.y0) / cfg.alpha;  // sub-cell height
        long mask = 0L;
        for (int row = 0; row < cfg.alpha; row++) {
            double sy0 = cell.y0 + row * chh;
            double sy1 = sy0 + chh;
            if (sy1 < q[1] || sy0 > q[3]) { continue; }   // sub-row misses window in y
            for (int col = 0; col < cfg.beta; col++) {
                double sx0 = cell.x0 + col * cw;
                double sx1 = sx0 + cw;
                if (sx1 < q[0] || sx0 > q[2]) { continue; } // misses in x
                mask |= (1L << (row * cfg.beta + col));
            }
        }
        return mask;
    }

    /**
     * Emit [lo,hi) TShape runs for the shape codes s in [0, 2^(a*b)) with
     * (s &amp; mask) != 0, offset into `code`'s block. Coalesces consecutive s.
     */
    private void emitShapeRuns(long code, long mask, List<LongRange> out) {
        long slots = cfg.shapeSlots();
        long base = code << cfg.shapeBits();
        long runLo = -1;
        for (long s = 0; s < slots; s++) {
            boolean keep = (s & mask) != 0L;
            if (keep && runLo < 0) {
                runLo = s;
            } else if (!keep && runLo >= 0) {
                out.add(new LongRange(base + runLo, base + s));
                runLo = -1;
            }
        }
        if (runLo >= 0) {
            out.add(new LongRange(base + runLo, base + slots));
        }
    }

    public List<LongRange> queryRanges(double[] window, int maxWork) {
        double[] q = {cfg.normX(window[0]), cfg.normY(window[1]),
                      cfg.normX(window[2]), cfg.normY(window[3])};
        List<LongRange> raw = new ArrayList<LongRange>();
        Deque<QuadElem> stack = new ArrayDeque<QuadElem>();
        // Seed from the ROOT itself, not its children: a trajectory whose bbox
        // straddles top-level quadrant boundaries is stored at a coarse ancestor
        // cell (possibly the root). Those ancestor blocks must be probed too, or
        // real matches stored there are dropped. Each ancestor overlapping the
        // query gets its shape block emitted, narrowed by the query's footprint
        // within THAT cell's own alpha x beta grid.
        stack.push(new QuadElem(new int[0], 0, 0, 1, 1));

        int work = 0;
        while (!stack.isEmpty() && work++ < maxWork) {
            QuadElem e = stack.pop();
            if (e.containedIn(q)) {
                long code = idx.quadrantCode(e.seq);
                long en = idx.enlargedNumber(e.seq.length);
                raw.add(new LongRange(idx.tShape(code, 0), idx.tShape(code + en, 0)));
            } else if (e.overlaps(q)) {
                long code = idx.quadrantCode(e.seq);
                long mask = queryMask(e, q);
                if (mask == 0L) {
                    // no sub-cell intersects (can happen only via enlarged extent);
                    // fall back to full block to stay safe
                    raw.add(new LongRange(idx.tShape(code, 0), idx.tShape(code + 1, 0)));
                } else if (mask == (cfg.shapeSlots() - 1)) {
                    // every sub-cell touched -> whole block, no narrowing gain
                    raw.add(new LongRange(idx.tShape(code, 0), idx.tShape(code + 1, 0)));
                } else {
                    emitShapeRuns(code, mask, raw);
                }
                if (e.seq.length < cfg.g) {
                    for (QuadElem c : e.children()) { stack.push(c); }
                }
            }
        }
        return merge(raw);
    }

    public List<LongRange> queryRanges(double[] window) {
        return queryRanges(window, 200_000);
    }

    private static List<LongRange> merge(List<LongRange> raw) {
        List<LongRange> out = new ArrayList<LongRange>();
        if (raw.isEmpty()) { return out; }
        raw.sort(new Comparator<LongRange>() {
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
