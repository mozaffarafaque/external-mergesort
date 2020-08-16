package com.mozafaq.extmergesort;

/**
 * @author Mozaffar Afaque
 */
public interface ResultRecordStream<T> {
    void accept(T record);
    void onBegin();
    void onComplete();
}
