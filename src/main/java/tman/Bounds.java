package tman;

/** An axis-aligned rectangle: (xmin, ymin, xmax, ymax). Used for MBRs, query
 *  rectangles, and enlarged-element/cell footprints alike. */
final class Bounds {
    public final double xmin;
    public final double ymin;
    public final double xmax;
    public final double ymax;

    public Bounds(double xmin, double ymin, double xmax, double ymax) {
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
    }

    @Override
    public String toString() {
        return "Bounds[" + xmin + ", " + ymin + ", " + xmax + ", " + ymax + "]";
    }
}