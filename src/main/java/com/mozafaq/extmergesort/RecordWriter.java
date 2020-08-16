package com.mozafaq.extmergesort;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Mozaffar Afaque
 */
public interface RecordWriter<T> extends BoundedStreamAware {

    void writeRecord(OutputStream outputStream, T record) throws IOException;
}
