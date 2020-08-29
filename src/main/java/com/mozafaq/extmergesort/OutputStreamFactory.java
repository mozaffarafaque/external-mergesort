package com.mozafaq.extmergesort;


import java.util.Objects;
import java.util.UUID;

/**
 * @author Mozaffar Afaque
 */
public class OutputStreamFactory {

    public static StreamWriter newStreamWriter(IOLocation location, String tempLocation, BoundedStreamAware boundedStreamAware) {
        Objects.requireNonNull(location);
        Objects.requireNonNull(location.getIoLocationType());

        switch (location.getIoLocationType()) {
            case FILE_SYSTEM:
                Objects.requireNonNull(location.getFileSystemPath());
                Objects.requireNonNull(location.getObjectName());
                String fileLocation = String.format("%s/%s", location.getFileSystemPath(), location.getObjectName());
                return new FileSystemStreamWriter(fileLocation, boundedStreamAware);
            case RECORD_STREAM:
                return new RecordStreamWriter(boundedStreamAware);
        }

        throw new IllegalStateException(location.getIoLocationType() + " case is not implemented");
    }

    public static StreamWriter getTempStreamWriter(String path, BoundedStreamAware boundedStreamAware) {
        Objects.requireNonNull(path);
        String objectName = UUID.randomUUID().toString() + ".tmp";
        return new FileSystemStreamWriter(String.format("%s/%s", path, objectName), boundedStreamAware);
    }

}
