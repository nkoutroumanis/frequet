package vre.impl;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;

import java.io.Serializable;

public final class SignatureCoder implements Serializable {

    private final int m;   // rows (latitude bands)
    private final int n;   // cols (longitude bands)

    public SignatureCoder(int m, int n) {
        if (m < 1 || n < 1) {
            throw new IllegalArgumentException("m,n must be >= 1");
        }
        this.m = m;
        this.n = n;
    }

    public int rows() { return m; }
    public int cols() { return n; }
    public int regionCount() { return m * n; }
    public int byteLength() { return (m * n + 7) >> 3; }

    public byte[] signature(SpatialPoint[] pts, double xmin, double ymin, double xmax, double ymax) {
        double w = xmax - xmin;
        double h = ymax - ymin;

        byte[] sig = new byte[byteLength()];

        for (SpatialPoint p : pts) {
            int col = bucket(p.getLongitude(), xmin, w, n);
            int row = bucket(p.getLatitude(), ymin, h, m);
            int idx = row * n + col;
            sig[idx >> 3] |= (byte) (1 << (idx & 7));
        }
        return sig;
    }

    private static int bucket(double v, double lo, double span, int count) {
        if (span <= 0.0) {
            return 0;
        }
        int b = (int) (((v - lo) / span) * count);
        if (b < 0) {
            b = 0;
        } else if (b >= count) {
            b = count - 1;
        }
        return b;
    }
}
