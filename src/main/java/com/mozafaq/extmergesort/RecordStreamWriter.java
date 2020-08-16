package com.mozafaq.extmergesort;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Mozaffar Afaque
 */
public class RecordStreamWriter implements StreamWriter {

    private final OutputStream outputStream = OutputStream.nullOutputStream();

    private BoundaryAware boundaryAware;

    RecordStreamWriter(BoundaryAware boundaryAware) {
        this.boundaryAware = boundaryAware;
    }

    @Override
    public OutputStream open() throws IOException {
        boundaryAware.onBegin();
        return outputStream;
    }

    @Override
    public OutputStream get() {
        return outputStream;
    }

    @Override
    public String getFullPath() {
        return null;
    }

    @Override
    public void close() throws IOException {
        boundaryAware.onComplete();

    }
}
