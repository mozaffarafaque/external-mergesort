package com.mozafaq.extmergesort;

/**
 * @author Mozaffar Afaque
 */
public interface StreamResultOutputHandler<T> {
    void accept(T record);
    void onBegin();
    void onComplete();
}
