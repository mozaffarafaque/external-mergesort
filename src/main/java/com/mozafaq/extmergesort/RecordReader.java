package com.mozafaq.extmergesort;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Mozaffar Afaque
 */
public interface RecordReader<T> {
    T readRecord(InputStream inputStream) throws IOException;
}
