package com.mozafaq.extmergesort;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Mozaffar Afaque
 */
public interface StreamWriter extends Closeable {
    OutputStream open() throws IOException;
    OutputStream get();
    String getFullPath();
}
