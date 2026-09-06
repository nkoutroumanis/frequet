package tman.impl;

import java.util.List;

public final class SelfTest {

    private static void check(boolean cond, String msg) {
        if (!cond) {
            throw new AssertionError("FAILED: " + msg);
        }
        System.out.println("OK  " + msg);
    }

    public static void main(String[] args) {
        // Equation 2 anchors -------------------------------------------------
        XZTrajectoryIndex g2 =
                new XZTrajectoryIndex(new XZConfig(2, 1, 2, 0, 1, 0, 1));
        check(g2.quadrantCode(new int[]{3, 3}) == 20, "code('33') g=2 == 20 (paper)");
        check(g2.quadrantCode(new int[]{0, 3}) == 5,  "code('03') g=2 == 5");
        System.out.print("children of '0' g=2 = ");
        for (int d = 0; d < 4; d++) {
            System.out.print(g2.quadrantCode(new int[]{0, d}) + " ");
        }
        System.out.println("(expect 2 3 4 5)");

        XZTrajectoryIndex g3 =
                new XZTrajectoryIndex(new XZConfig(3, 1, 2, 0, 1, 0, 1));
        check(g3.quadrantCode(new int[]{3, 1}) == 70,      "code('31') g=3 == 70");
        check(g3.quadrantCode(new int[]{3, 1, 0}) == 71,   "code('310') g=3 == 71");
        check(g3.quadrantCode(new int[]{3, 2}) == 75,      "code('32') g=3 == 75 (next sibling)");
        check(g3.enlargedNumber(2) == 5,                   "EN('31') g=3 == 5");

        // Equation 3 packing, alpha*beta = 2 -> code 5 owns 20..23 -----------
        XZTrajectoryIndex c2 =
                new XZTrajectoryIndex(new XZConfig(3, 1, 2, 0, 1, 0, 1));
        check(c2.tShape(5, 0) == 20 && c2.tShape(5, 3) == 23,
                "TShape(5,0..3) a*b=2 == 20..23");
        check(c2.tShape(6, 0) == 24, "TShape(6,0) a*b=2 == 24 (next block)");

        // Equation 3 packing, 3x3 -> 512 slots -------------------------------
        XZConfig cfg9 = new XZConfig(3, 3, 3, 0, 1, 0, 1);
        XZTrajectoryIndex c9 = new XZTrajectoryIndex(cfg9);
        check(cfg9.shapeSlots() == 512, "a*b=9 -> 512 shape slots");
        check(c9.tShape(5, 0) == 2560,   "TShape(5,0) a*b=9 == 2560");
        check(c9.tShape(5, 511) == 3071, "TShape(5,511) a*b=9 == 3071");
        check(c9.tShape(6, 0) == 3072,   "TShape(6,0) a*b=9 == 3072");

        // Query -> ranges ----------------------------------------------------
        XZTrajectoryIndex idx =
                new XZTrajectoryIndex(new XZConfig(6, 3, 3, 0, 1, 0, 1));
        List<LongRange> r = idx.queryRanges(new double[]{0.2, 0.2, 0.6, 0.6});
        boolean ordered = true;
        for (int i = 0; i < r.size(); i++) {
            if (r.get(i).lo >= r.get(i).hi) {
                ordered = false;
            }
            if (i > 0 && r.get(i - 1).hi > r.get(i).lo) {
                ordered = false;
            }
        }
        check(ordered && !r.isEmpty(), "query ranges half-open & ascending, n=" + r.size());
        System.out.println("SQL: "
                + XZTrajectoryIndex.rangesToSql(r.subList(0, Math.min(2, r.size())), "idx"));

        System.out.println("\nALL CHECKS PASSED");



//        int[]  seq  = idx.homeCellSequence(OccupancyShapeCoder.bbox(points)); // the covering cell
//        long   code = idx.quadrantCode(seq);                                  // Equation 2
//        long   s    = OccupancyShapeCoder.shapeCodeForTrajectory(points, seq, cfg); // shape bitmap
//        long   tshape = idx.tShape(code, s);

//        CachedShapeQuery csq = new CachedShapeQuery(cfg, idx, myCache);
//        List<LongRange> ranges = csq.queryRanges(new double[]{xmin, ymin, xmax, ymax});
//
//        FrechetCachedQuery fcq = new FrechetCachedQuery(cfg, idx, myCache);
//        List<LongRange> ranges = fcq.queryRanges(Q, theta);
    }
}
