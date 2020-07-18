package com.mozafaq.extmergesort;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Mozaffar Afaque
 */
public interface StreamReader extends Closeable {
    InputStream open() throws IOException;
    InputStream get();
    String getFullPath();
}
