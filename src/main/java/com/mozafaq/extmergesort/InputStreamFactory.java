package com.mozafaq.extmergesort;

import com.amazonaws.auth.AWSCredentials;

import java.util.Objects;

/**
 * @author Mozaffar Afaque
 */
public class InputStreamFactory {

    public static StreamReader newStreamReader(IOLocation location) {
        Objects.requireNonNull(location);
        Objects.requireNonNull(location.getIoLocationType());

        switch (location.getIoLocationType()) {
            case FILE_SYSTEM:
                Objects.requireNonNull(location.getFileSystemPath());
                Objects.requireNonNull(location.getObjectName());

                return new FileSystemStreamReader(
                        String.format("%s/%s",
                                location.getFileSystemPath(),
                                location.getObjectName())
                );
        }

        throw new IllegalStateException(location.getIoLocationType() + " case is not implemented");
    }

    public static StreamReader getTempStreamReader(String fileLocation) {
        Objects.requireNonNull(fileLocation);
        return new FileSystemStreamReader(fileLocation);
    }
}
