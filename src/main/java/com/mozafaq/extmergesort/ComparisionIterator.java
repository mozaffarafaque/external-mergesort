package com.mozafaq.extmergesort;

import java.util.Comparator;
import java.util.Iterator;

/**
 * @author Mozaffar Afaque
 */
public interface ComparisionIterator<T> extends Iterator<T> {
    T current();
    default int compareToCurrent(Comparator<T> comparator, T other) {

        T thisOne = current();
        if (thisOne == null) {
            return -1;
        }
        return comparator.compare(thisOne, other);
    };
}
