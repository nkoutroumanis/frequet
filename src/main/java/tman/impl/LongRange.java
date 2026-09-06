package tman.impl;

public final class LongRange {
    public final long lo;   // inclusive
    public final long hi;   // exclusive

    public LongRange(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
    }

    @Override
    public String toString() {
        return "[" + lo + ", " + hi + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LongRange)) {
            return false;
        }
        LongRange r = (LongRange) o;
        return lo == r.lo && hi == r.hi;
    }

    @Override
    public int hashCode() {
        return (int) (lo * 31 + hi);
    }
}
