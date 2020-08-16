package com.mozafaq.extmergesort;

public interface BoundaryAware {

    default void onBegin() {
        // Do nothing
    }

    default void onComplete() {
        // Do nothing
    }
}
