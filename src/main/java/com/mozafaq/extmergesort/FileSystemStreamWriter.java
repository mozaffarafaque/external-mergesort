package com.mozafaq.extmergesort;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Mozaffar Afaque
 */
public class FileSystemStreamWriter implements StreamWriter {
    private String fileLocation ;
    private OutputStream outputStream = null;
    private boolean isClosed;
    private BoundedStreamAware boundedStreamAware;

    public FileSystemStreamWriter(String fileLocation, BoundedStreamAware boundedStreamAware) {
        this.fileLocation = fileLocation;
        this.boundedStreamAware = boundedStreamAware;
        isClosed = false;
    }

    @Override
    public OutputStream open() throws IOException {
        if (outputStream != null) {
            throw new IllegalStateException("Stream is already open!");
        }
        outputStream = new FileOutputStream(fileLocation);
        boundedStreamAware.onBegin();
        return outputStream;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            boundedStreamAware.onComplete();
            outputStream.close();
            isClosed = true;
        }
    }

    @Override
    public OutputStream get() {
        return outputStream;
    }

    @Override
    public String getFullPath() {
        return fileLocation;
    }

}
