package com.mozafaq.extmergesort;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author Mozaffar Afaque
 */
public final class ComparisionAwareKey implements Comparable<ComparisionAwareKey>, Serializable {

    private final int hashCode;
    private final Comparable<?>[] attributes;

    private ComparisionAwareKey(Comparable<?>[] attributes) {
        this.attributes = attributes;
        hashCode = Arrays.hashCode(this.attributes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        return (obj instanceof ComparisionAwareKey) &&
                Arrays.equals(this.attributes, ((ComparisionAwareKey) obj).attributes);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public int compareTo(ComparisionAwareKey that) {
        if (that == null) {
            return 1;
        }

        int minSize = Math.min(this.attributes.length, that.attributes.length);
        for (int i = 0; i < minSize; i++) {
            Comparable<?> thisKey = this.attributes[i];
            Comparable<?> thatKey = that.attributes[i];
            if (Objects.equals(thisKey, thatKey)) {
                continue;
            } else if (thisKey == null) {
                return -1;
            } else if (thatKey == null) {
                return 1;
            } else {
                return ((Comparable) thisKey).compareTo(thatKey);
            }
        }
        if (this.attributes.length == that.attributes.length) {
            return 0;
        }
        return this.attributes.length > that.attributes.length ? 1 : -1;
    }

    public static ComparisionAwareKey create(Comparable<?>... keyAttributes) {
        if (keyAttributes == null ) {
            return null;
        }

        return new ComparisionAwareKey(keyAttributes);
    }

    @Override
    public String toString() {
        return "[" + Arrays.toString(this.attributes) + "]";
    }

}
