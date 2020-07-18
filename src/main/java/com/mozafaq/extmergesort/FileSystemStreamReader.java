package com.mozafaq.extmergesort;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Mozaffar Afaque
 */
public class FileSystemStreamReader implements StreamReader {
    InputStream inputStream = null;
    private String location;

    public FileSystemStreamReader(String location) {
        this.location = location;
    }

    @Override
    public InputStream open() throws IOException {
        if (inputStream != null) {
            throw new IllegalStateException("Stream is already open!");
        }
        inputStream = new FileInputStream(location);
        return inputStream;
    }

    @Override
    public void close() throws IOException{
        inputStream.close();
    }

    @Override
    public InputStream get() {
        return inputStream;
    }

    @Override
    public String getFullPath() {
        return location;
    }
}
