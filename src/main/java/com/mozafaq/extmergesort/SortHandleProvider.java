package com.mozafaq.extmergesort;

import java.util.Comparator;

/**
 * Needs to be provided for various sorting related operations
 *
 * @author Mozaffar Afaque
 */
public interface SortHandleProvider<T> {

    /**
     * Handler for how an object is written to <code>OutputStream</code>.
     *
     * @return write handler object.
     */
    RecordWriter<T> recordWriter();

    /**
     * Handler for how an object is read from <code>InputStream</code>.
     *
     * @return read handler object.
     */
    RecordReader<T> recordReader();

    /**
     * Comparator for comparing two objects, This is used for sorting and decides the order
     * or sorting.
     *
     * @return comparator implemented object.
     */
    Comparator<T> recordComparator();

    /**
     * This should be returned as null if you don't want output sas stream.
     *
     * @return output handler if output is supposed to be stream.
     */
    StreamResultOutputHandler<T> streamResultOutputHandler();
}
