package com.mozafaq.extmergesort;

/**
 * @author Mozaffar Afaque
 */
public interface BoundedStreamAware {

    default void onBegin() {
        // Do nothing
    }

    default void onComplete() {
        // Do nothing
    }
}
