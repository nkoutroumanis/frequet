package gr.ds.unipi.spatialnodb.messages.common;

import java.io.Serializable;
import java.util.Objects;

public class HilbertKeyLongitude implements Comparable<HilbertKeyLongitude>, Serializable {
    public long getHilbertKey() {
        return hilbertKey;
    }

    public double getLongitude() {
        return longitude;
    }

    private final long hilbertKey;
        private final double longitude;

        public HilbertKeyLongitude(long hilbertKey, double longitude) {
            this.hilbertKey = hilbertKey;
            this.longitude = longitude;
        }

        @Override
        public int compareTo(HilbertKeyLongitude o) {
            int c = Long.compare(this.hilbertKey, o.hilbertKey);
            if (c != 0) return c;
            return Double.compare(this.longitude, o.longitude);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof HilbertKeyLongitude)) return false;
            HilbertKeyLongitude other = (HilbertKeyLongitude) o;
            return this.hilbertKey == other.hilbertKey &&
                    this.longitude == other.longitude;
        }

    @Override
    public int hashCode() {
        return Objects.hash(hilbertKey, longitude);
    }
}
