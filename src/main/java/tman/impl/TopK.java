package tman.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public final class TopK {

    public interface SignatureCache {
        long[] signaturesForCell(long quadrantCode);
    }

    public interface RegionReader {
        /** Trajectories stored under tShape(code, signature); never null. */
        List<double[][]> read(long quadrantCode, long signature);
    }

    /** Paper's filter(indexSpace) at lines 11/13 (Lemmas 10-11 + local filtering).
     *  Return true to PRUNE (skip) this index space. */
    public interface IndexSpaceFilter {
        boolean prune(long code, long signature, double minDistIS, double epsilon);
    }

    /** Paper's filter(enlargedElement) at line 16 (Lemmas 6-9).
     *  Return true to PRUNE this cell (do not expand or add its index spaces). */
    public interface CellFilter {
        boolean prune(int[] seq, double minDistEE, double epsilon);
    }

    private final XZConfig cfg;
    private final XZTrajectoryIndex idx;
    private final SignatureCache cache;
    private final RegionReader reader;
    private final IndexSpaceFilter isFilter;
    private final CellFilter cellFilter;

    public TopK(XZConfig cfg, XZTrajectoryIndex idx, SignatureCache cache,
                     RegionReader reader) {
        this(cfg, idx, cache, reader,
                // default index-space filter = Lemma 11: prune if minDistIS > epsilon
                (code, sig, mdis, eps) -> mdis > eps,
                // default cell filter = Lemmas 8-9: prune if minDistEE > epsilon
                (seq, mdee, eps) -> mdee > eps);
    }

    public TopK(XZConfig cfg, XZTrajectoryIndex idx, SignatureCache cache,
                     RegionReader reader,
                     IndexSpaceFilter isFilter, CellFilter cellFilter) {
        this.cfg = cfg; this.idx = idx; this.cache = cache; this.reader = reader;
        this.isFilter = isFilter; this.cellFilter = cellFilter;
    }

    public static final class Result {
        public final double[][] trajectory;
        public final double distance;
        public Result(double[][] t, double d) { this.trajectory = t; this.distance = d; }
    }

    private static final class CellEntry {
        final int[] seq; final double x0, y0, x1, y1; final double minDistEE;
        CellEntry(int[] seq, double x0, double y0, double x1, double y1, double m) {
            this.seq=seq; this.x0=x0; this.y0=y0; this.x1=x1; this.y1=y1; this.minDistEE=m;
        }
    }
    private static final class ISEntry {
        final long code; final long signature; final double minDistIS;
        ISEntry(long c, long s, double m){ this.code=c; this.signature=s; this.minDistIS=m; }
    }

    private static double pointRectDist(double px,double py,double x0,double y0,double x1,double y1){
        double dx = px<x0?x0-px:(px>x1?px-x1:0.0);
        double dy = py<y0?y0-py:(py>y1?py-y1:0.0);
        return Math.sqrt(dx*dx+dy*dy);
    }
    private double minDist(double x0,double y0,double x1,double y1,double[][] qn){
        double best=Double.POSITIVE_INFINITY;
        for(double[] p:qn){ double d=pointRectDist(p[0],p[1],x0,y0,x1,y1); if(d<best)best=d; if(best==0)break; }
        return best;
    }
    private double minDistIS(double cx0,double cy0,double cx1,double cy1,long sig,double[][] qn){
        double cw=(cx1-cx0)/cfg.beta, ch=(cy1-cy0)/cfg.alpha, best=Double.POSITIVE_INFINITY;
        long bits=sig;
        while(bits!=0){ int b=Long.numberOfTrailingZeros(bits); bits&=(bits-1);
            int row=b/cfg.beta, col=b%cfg.beta;
            double sx0=cx0+col*cw, sy0=cy0+row*ch;
            double d=minDist(sx0,sy0,sx0+cw,sy0+ch,qn); if(d<best)best=d; if(best==0)break; }
        return best;
    }
    private static double[] childBounds(double x0,double y0,double x1,double y1,int q){
        double mx=(x0+x1)/2.0,my=(y0+y1)/2.0; boolean r=(q&1)==1,t=(q&2)==2;
        return new double[]{ r?mx:x0, t?my:y0, r?x1:mx, t?y1:my };
    }

    private static List<Result> drain(PriorityQueue<Result> pq){
        List<Result> out=new ArrayList<Result>(pq);
        out.sort((a,b)->Double.compare(a.distance,b.distance));
        return out;
    }
}