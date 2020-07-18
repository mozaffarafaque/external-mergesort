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

    public FileSystemStreamWriter(String fileLocation) {
        this.fileLocation = fileLocation;
        isClosed = false;
    }

    @Override
    public OutputStream open() throws IOException {
        if (outputStream != null) {
            throw new IllegalStateException("Stream is already open!");
        }
        outputStream = new FileOutputStream(fileLocation);
        return outputStream;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
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
