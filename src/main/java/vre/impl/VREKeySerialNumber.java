package vre.impl;

import java.io.Serializable;
import java.util.Objects;

public class VREKeySerialNumber implements Comparable<VREKeySerialNumber>, Serializable {
    public long getKey() {
        return key;
    }

    public int getSerialNumber() {
        return serialNumber;
    }

    private final long key;
    private final int serialNumber;

    public VREKeySerialNumber(long key, int serialNumber) {
        this.key = key;
        this.serialNumber = serialNumber;
    }

        @Override
        public int compareTo(VREKeySerialNumber o) {
            int c = Long.compare(this.key, o.key);
            if (c != 0) return c;
            return Integer.compare(this.serialNumber, o.serialNumber);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof VREKeySerialNumber)) return false;
            VREKeySerialNumber other = (VREKeySerialNumber) o;
            return this.key == other.key &&
                    this.serialNumber == other.serialNumber;
        }

    @Override
    public int hashCode() {
        return Objects.hash(key, serialNumber);
    }
}
