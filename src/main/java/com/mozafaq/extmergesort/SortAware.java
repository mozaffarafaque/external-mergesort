package com.mozafaq.extmergesort;

import java.util.Comparator;

/**
 * @author Mozaffar Afaque
 */
public interface SortAware<T> {
    RecordWriter<T> recordWriter();
    RecordReader<T> recordReader();
    Comparator<T> recordComparator();
    ResultRecordStream<T> resultRecordStream();
}
