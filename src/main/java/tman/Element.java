package tman;

import java.util.Objects;

/** An enlarged element: the lower-left quadtree cell (cx, cy) at resolution
 *  r that anchors an alpha x beta window. Immutable value type used as a
 *  HashMap/HashSet key throughout TShapeIndex, so equals()/hashCode() matter. */
final class Element {
    public final int cx;
    public final int cy;
    public final int r;

    public Element(int cx, int cy, int r) {
        this.cx = cx;
        this.cy = cy;
        this.r = r;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Element)) {
            return false;
        }
        Element other = (Element) o;
        return cx == other.cx && cy == other.cy && r == other.r;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cx, cy, r);
    }

    @Override
    public String toString() {
        return "(" + cx + ", " + cy + ", " + r + ")";
    }
}