package vre.impl;

import java.io.Serializable;
import java.util.*;

public final class XZ2Coder implements Serializable {

    public final int g;                 // max quadtree resolution
    private final double xMin, xMax, yMin, yMax;

    public XZ2Coder(int g, double xMin, double xMax, double yMin, double yMax) {
        if (g < 1) {
            throw new IllegalArgumentException("g must be >= 1");
        }
        this.g = g;
        this.xMin = xMin;
        this.xMax = xMax;
        this.yMin = yMin;
        this.yMax = yMax;
    }

    public long code(double xmin, double ymin, double xmax, double ymax) {
        int[] seq = homeCellSequence(xmin, ymin, xmax, ymax);
        return quadrantCode(seq);
    }

    public long code(double[] bbox) {
        return code(bbox[0], bbox[1], bbox[2], bbox[3]);
    }

    private double normX(double x) {
        return (x - xMin) / (xMax - xMin);
    }

    private double normY(double y) {
        return (y - yMin) / (yMax - yMin);
    }

    private static long subtreeSize(int levelsBelow) {
        long p = 1L;
        for (int i = 0; i < levelsBelow + 1; i++) {
            p *= 4L;
        }
        return (p - 1L) / 3L;
    }

    public long quadrantCode(int[] seq) {
        long code = 0L;
        for (int i = 1; i <= seq.length; i++) {          // i = 1..r
            int q = seq[i - 1];
            code += 1L + (long) q * subtreeSize(g - i);   // (4^(g-i+1)-1)/3
        }
        return code;
    }

    public long enlargedNumber(int seqLen) {
        return subtreeSize(g - seqLen);
    }

    private int[] homeCellSequence(double xmin, double ymin, double xmax, double ymax) {
        double nxmin = normX(xmin);
        double nymin = normY(ymin);
        double nxmax = normX(xmax);
        double nymax = normY(ymax);

        List<Integer> seq = new ArrayList<Integer>();
        double cx0 = 0.0, cy0 = 0.0, cx1 = 1.0, cy1 = 1.0;
        for (int level = 0; level < g; level++) {
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

    public List<long[]> queryRanges(double wxmin, double wymin, double wxmax, double wymax) {
        double nwx0 = normX(wxmin), nwy0 = normY(wymin);
        double nwx1 = normX(wxmax), nwy1 = normY(wymax);

        List<long[]> ranges = new ArrayList<long[]>();

        // Each stack item is a cell: {depth, x0, y0, x1, y1} in normalized coords,
        // plus its quadrant sequence carried alongside for the code computation.
        Deque<int[]> seqStack = new ArrayDeque<int[]>();
        Deque<double[]> boxStack = new ArrayDeque<double[]>();
        seqStack.push(new int[0]);
        boxStack.push(new double[]{0.0, 0.0, 1.0, 1.0});

        while (!seqStack.isEmpty()) {
            int[] seq = seqStack.pop();
            double[] b = boxStack.pop();
            double x0 = b[0], y0 = b[1], x1 = b[2], y1 = b[3];

            // enlarged element: cell doubled toward upper-right
            double ex1 = x1 + (x1 - x0);
            double ey1 = y1 + (y1 - y0);

            boolean enlargedOverlaps =
                    !(ex1 < nwx0 || x0 > nwx1 || ey1 < nwy0 || y0 > nwy1);
            if (!enlargedOverlaps) {
                continue;   // neither this cell nor its subtree can reach the window
            }

            // this cell's enlarged element overlaps -> emit its home-cell code
            long code = quadrantCode(seq);
            ranges.add(new long[]{code, code + 1});

            // recurse on ENLARGED overlap (NOT gated on actual-cell overlap)
            if (seq.length < g) {
                double mx = (x0 + x1) / 2.0;
                double my = (y0 + y1) / 2.0;
                double[][] children = {
                        {x0, y0, mx, my},   // q=0 bottom-left
                        {mx, y0, x1, my},   // q=1 bottom-right
                        {x0, my, mx, y1},   // q=2 top-left
                        {mx, my, x1, y1}    // q=3 top-right
                };
                for (int q = 0; q < 4; q++) {
                    int[] cs = new int[seq.length + 1];
                    System.arraycopy(seq, 0, cs, 0, seq.length);
                    cs[seq.length] = q;
                    seqStack.push(cs);
                    boxStack.push(children[q]);
                }
            }
        }

        // sort and coalesce adjacent/overlapping ranges to shrink the predicate
        if (ranges.isEmpty()) {
            return ranges;
        }
        ranges.sort(new Comparator<long[]>() {
            @Override public int compare(long[] a, long[] b) {
                return Long.compare(a[0], b[0]);
            }
        });
        List<long[]> merged = new ArrayList<long[]>();
        long[] cur = ranges.get(0).clone();
        for (int i = 1; i < ranges.size(); i++) {
            long[] r = ranges.get(i);
            if (r[0] <= cur[1]) {                 // overlap or adjacent -> merge
                if (r[1] > cur[1]) cur[1] = r[1];
            } else {
                merged.add(cur);
                cur = r.clone();
            }
        }
        merged.add(cur);
        return merged;
    }

    public List<long[]> queryRanges(double[] window) {
        return queryRanges(window[0], window[1], window[2], window[3]);
    }

    public List<long[]> queryRanges(double wxmin, double wymin, double wxmax, double wymax, int maxRanges) {
        if (maxRanges < 1) {
            throw new IllegalArgumentException("maxRanges must be >= 1");
        }
        double nwx0 = normX(wxmin), nwy0 = normY(wymin);
        double nwx1 = normX(wxmax), nwy1 = normY(wymax);

        List<long[]> chosen = null;

        // Try depth caps 0..g; keep the deepest result that fits within maxRanges.
        for (int maxDepth = 0; maxDepth <= g; maxDepth++) {
            List<long[]> ranges = new ArrayList<long[]>();
            Deque<int[]> seqStack = new ArrayDeque<int[]>();
            Deque<double[]> boxStack = new ArrayDeque<double[]>();
            seqStack.push(new int[0]);
            boxStack.push(new double[]{0.0, 0.0, 1.0, 1.0});

            while (!seqStack.isEmpty()) {
                int[] seq = seqStack.pop();
                double[] b = boxStack.pop();
                double x0 = b[0], y0 = b[1], x1 = b[2], y1 = b[3];

                // enlarged element: cell doubled toward upper-right
                double ex1 = x1 + (x1 - x0);
                double ey1 = y1 + (y1 - y0);
                boolean enlargedOverlaps =
                        !(ex1 < nwx0 || x0 > nwx1 || ey1 < nwy0 || y0 > nwy1);
                if (!enlargedOverlaps) {
                    continue;
                }

                long code = quadrantCode(seq);
                int depth = seq.length;

                if (depth >= maxDepth || depth >= g) {
                    // stop here: emit the whole subtree range (this cell + descendants)
                    ranges.add(new long[]{code, code + enlargedNumber(depth)});
                } else {
                    // emit this cell's own code, and recurse
                    ranges.add(new long[]{code, code + 1});
                    double mx = (x0 + x1) / 2.0;
                    double my = (y0 + y1) / 2.0;
                    double[][] children = {
                            {x0, y0, mx, my},   // q=0 bottom-left
                            {mx, y0, x1, my},   // q=1 bottom-right
                            {x0, my, mx, y1},   // q=2 top-left
                            {mx, my, x1, y1}    // q=3 top-right
                    };
                    for (int q = 0; q < 4; q++) {
                        int[] cs = new int[seq.length + 1];
                        System.arraycopy(seq, 0, cs, 0, seq.length);
                        cs[seq.length] = q;
                        seqStack.push(cs);
                        boxStack.push(children[q]);
                    }
                }
            }

            // sort and coalesce adjacent/overlapping ranges
            List<long[]> merged = new ArrayList<long[]>();
            if (!ranges.isEmpty()) {
                ranges.sort(new Comparator<long[]>() {
                    @Override public int compare(long[] a, long[] b) {
                        return Long.compare(a[0], b[0]);
                    }
                });
                long[] cur = ranges.get(0).clone();
                for (int i = 1; i < ranges.size(); i++) {
                    long[] r = ranges.get(i);
                    if (r[0] <= cur[1]) {                 // overlap or adjacent -> merge
                        if (r[1] > cur[1]) cur[1] = r[1];
                    } else {
                        merged.add(cur);
                        cur = r.clone();
                    }
                }
                merged.add(cur);
            }

            if (merged.size() <= maxRanges) {
                chosen = merged;      // fits -> keep (deeper is more precise)
            } else {
                break;                // deeper only grows the count; stop
            }
        }

        return (chosen == null) ? new ArrayList<long[]>() : chosen;
    }
}
