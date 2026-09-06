package tman.impl;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

public final class XZTrajectoryIndex implements Serializable {
    public interface ShapeCoder extends Serializable {
        long shapeCode(double[] bbox, int[] cellSeq, XZConfig cfg);
    }

    public static final ShapeCoder ZERO_SHAPE = (bbox, cellSeq, cfg) -> 0L;

    private final XZConfig cfg;
    private final ShapeCoder shapeCoder;

    public XZTrajectoryIndex(XZConfig cfg) {
        this(cfg, ZERO_SHAPE);
    }

    public XZTrajectoryIndex(XZConfig cfg, ShapeCoder shapeCoder) {
        this.cfg = cfg;
        this.shapeCoder = shapeCoder;
    }

    static long subtreeSize(int levelsBelow) {
        long p = 1L;
        for (int i = 0; i < levelsBelow + 1; i++) {
            p *= 4L;
        }
        return (p - 1L) / 3L;
    }

    public long quadrantCode(int[] seq) {
        long code = 0L;
        for (int i = 1; i <= seq.length; i++) {      // i = 1..r
            int q = seq[i - 1];
            if (q < 0 || q > 3) {
                throw new IllegalArgumentException("quadrant digit out of range: " + q);
            }
            code += 1L + (long) q * subtreeSize(cfg.g - i);   // (4^(g-i+1)-1)/3
        }
        return code;
    }

    /**
     * EN(E): number of cells in E's own subtree (E plus all descendants down to
     * level g). For a cell at depth {@code seqLen} that's subtree(g - seqLen).
     */
    public long enlargedNumber(int seqLen) {
        return subtreeSize(cfg.g - seqLen);
    }

    // ------------------------------------------------------ Equation 3: TShape

    /** Equation 3: pack quadrant code (high bits) with shape code s (low bits). */
    public long tShape(long code, long s) {
        if (s < 0 || s >= cfg.shapeSlots()) {
            throw new IllegalArgumentException(
                    "shape code " + s + " out of range [0," + cfg.shapeSlots() + ")");
        }
        return (code << cfg.shapeBits()) | s;
    }

    // ---------------------------------------- home cell (smallest containing quad)

    /**
     * Smallest quadrant fully containing the bbox. Descends until the box would
     * straddle a child boundary, or depth g is reached.
     *
     * @param bbox (xmin, ymin, xmax, ymax) in USER coordinates
     * @return the home cell's quadrant sequence (may be empty = whole domain)
     */
    public int[] homeCellSequence(double[] bbox) {
        double nxmin = cfg.normX(bbox[0]);
        double nymin = cfg.normY(bbox[1]);
        double nxmax = cfg.normX(bbox[2]);
        double nymax = cfg.normY(bbox[3]);

        List<Integer> seq = new ArrayList<Integer>();
        double cx0 = 0.0, cy0 = 0.0, cx1 = 1.0, cy1 = 1.0;
        for (int level = 0; level < cfg.g; level++) {
            double mx = (cx0 + cx1) / 2.0;
            double my = (cy0 + cy1) / 2.0;
            // box must lie wholly on one side of each mid-line to descend
            boolean straddleX = (nxmin < mx) != (nxmax < mx);
            boolean straddleY = (nymin < my) != (nymax < my);
            if (straddleX || straddleY) {
                break;
            }
            boolean right = nxmin >= mx;
            boolean top = nymin >= my;
            int q = (top ? 2 : 0) + (right ? 1 : 0);
            seq.add(q);
            if (right) {
                cx0 = mx;
            } else {
                cx1 = mx;
            }
            if (top) {
                cy0 = my;
            } else {
                cy1 = my;
            }
        }
        int[] out = new int[seq.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = seq.get(i);
        }
        return out;
    }

    // ------------------------------------------------ PUBLIC: index a trajectory

    /** Full pipeline: trajectory bbox -&gt; single packed long index value. */
    public long indexTrajectory(double[] bbox) {
        int[] seq = homeCellSequence(bbox);
        long code = quadrantCode(seq);
        long s = shapeCoder.shapeCode(bbox, seq, cfg);
        return tShape(code, s);
    }

    // ================================================= QUERY -> RANGES

    /** An XZ "enlarged" quad element used only during query decomposition. */
    private static final class QuadElem {
        final int[] seq;
        final double x0, y0, x1, y1;

        QuadElem(int[] seq, double x0, double y0, double x1, double y1) {
            this.seq = seq;
            this.x0 = x0;
            this.y0 = y0;
            this.x1 = x1;
            this.y1 = y1;
        }

        double extX1() {
            return x1 + (x1 - x0);   // enlarged: upper bound extended by width
        }

        double extY1() {
            return y1 + (y1 - y0);
        }

        boolean containedIn(double[] q) {
            return q[0] <= x0 && q[1] <= y0 && q[2] >= extX1() && q[3] >= extY1();
        }

        boolean overlaps(double[] q) {
            return q[2] >= x0 && q[3] >= y0 && q[0] <= extX1() && q[1] <= extY1();
        }

        QuadElem[] children() {
            double mx = (x0 + x1) / 2.0;
            double my = (y0 + y1) / 2.0;
            return new QuadElem[]{
                    new QuadElem(append(seq, 0), x0, y0, mx, my),
                    new QuadElem(append(seq, 1), mx, y0, x1, my),
                    new QuadElem(append(seq, 2), x0, my, mx, y1),
                    new QuadElem(append(seq, 3), mx, my, x1, y1)
            };
        }

        private static int[] append(int[] a, int v) {
            int[] b = new int[a.length + 1];
            System.arraycopy(a, 0, b, 0, a.length);
            b[a.length] = v;
            return b;
        }
    }

    /**
     * Decompose a spatial window into a minimal set of [lo, hi) packed-long ranges.
     *
     * <p>Fully-contained cell E -&gt; whole subtree:
     *   [ tShape(code(E), 0), tShape(code(E)+EN(E), 0) )
     * <p>Partially-overlapping E -&gt; just E's own block, then recurse into children:
     *   [ tShape(code(E), 0), tShape(code(E)+1, 0) )
     *
     * @param window (xmin, ymin, xmax, ymax) in USER coordinates
     * @param maxRanges rough cap on ranges before bottoming out
     * @return merged, ascending, half-open ranges on the packed index value
     */
    public List<LongRange> queryRanges(double[] window, int maxRanges) {
        double[] q = new double[]{
                cfg.normX(window[0]), cfg.normY(window[1]),
                cfg.normX(window[2]), cfg.normY(window[3])
        };

        List<LongRange> raw = new ArrayList<LongRange>();
        Deque<QuadElem> stack = new ArrayDeque<QuadElem>();
        for (QuadElem c : new QuadElem(new int[0], 0, 0, 1, 1).children()) {
            stack.push(c);
        }

        while (!stack.isEmpty() && raw.size() < maxRanges) {
            QuadElem e = stack.pop();
            if (e.containedIn(q)) {
                long code = quadrantCode(e.seq);
                long en = enlargedNumber(e.seq.length);
                raw.add(new LongRange(tShape(code, 0), tShape(code + en, 0)));
            } else if (e.overlaps(q)) {
                long code = quadrantCode(e.seq);
                raw.add(new LongRange(tShape(code, 0), tShape(code + 1, 0)));
                if (e.seq.length < cfg.g) {
                    for (QuadElem c : e.children()) {
                        stack.push(c);
                    }
                }
            }
            // else: no overlap -> drop
        }

        return merge(raw);
    }

    public List<LongRange> queryRanges(double[] window) {
        return queryRanges(window, 100_000);
    }

    private static List<LongRange> merge(List<LongRange> raw) {
        List<LongRange> out = new ArrayList<LongRange>();
        if (raw.isEmpty()) {
            return out;
        }
        raw.sort(new Comparator<LongRange>() {
            @Override
            public int compare(LongRange a, LongRange b) {
                int c = Long.compare(a.lo, b.lo);
                return c != 0 ? c : Long.compare(a.hi, b.hi);
            }
        });
        long curLo = raw.get(0).lo;
        long curHi = raw.get(0).hi;
        for (int i = 1; i < raw.size(); i++) {
            LongRange r = raw.get(i);
            if (r.lo <= curHi) {                 // overlapping or adjacent
                curHi = Math.max(curHi, r.hi);
            } else {
                out.add(new LongRange(curLo, curHi));
                curLo = r.lo;
                curHi = r.hi;
            }
        }
        out.add(new LongRange(curLo, curHi));
        return out;
    }

    // -------------------------------------------------------- store-query glue

    /** OR-of-bands SQL WHERE fragment (DuckDB / Spark over Parquet, etc.). */
    public static String rangesToSql(List<LongRange> ranges, String column) {
        if (ranges.isEmpty()) {
            return "FALSE";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ranges.size(); i++) {
            LongRange r = ranges.get(i);
            if (i > 0) {
                sb.append(" OR ");
            }
            sb.append('(').append(column).append(" >= ").append(r.lo)
              .append(" AND ").append(column).append(" < ").append(r.hi).append(')');
        }
        return sb.toString();
    }
}
