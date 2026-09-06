package tman.impl;

import java.io.Serializable;

public final class XZConfig implements Serializable {

    public final int g;
    public final int alpha;
    public final int beta;
    public final double xMin;
    public final double xMax;
    public final double yMin;
    public final double yMax;

    public XZConfig(int g, int alpha, int beta,
                    double xMin, double xMax, double yMin, double yMax) {
        if (g < 1) {
            throw new IllegalArgumentException("g must be >= 1");
        }
        if (alpha < 0 || beta < 0) {
            throw new IllegalArgumentException("alpha,beta must be >= 0");
        }
        this.g = g;
        this.alpha = alpha;
        this.beta = beta;
        this.xMin = xMin;
        this.xMax = xMax;
        this.yMin = yMin;
        this.yMax = yMax;
    }

    /** Whole-earth lon/lat convenience constructor. */
    public XZConfig(int g, int alpha, int beta) {
        this(g, alpha, beta, -180.0, 180.0, -90.0, 90.0);
    }

    /** Number of low bits reserved for the shape code s. */
    public int shapeBits() {
        return alpha * beta;
    }

    /** Block width per cell = 2^(alpha*beta) = number of possible shape codes. */
    public long shapeSlots() {
        return 1L << shapeBits();
    }

    public double normX(double x) {
        return (x - xMin) / (xMax - xMin);
    }

    public double normY(double y) {
        return (y - yMin) / (yMax - yMin);
    }
}
