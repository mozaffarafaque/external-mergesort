package com.mozafaq.extmergesort;

import java.util.Objects;

/**
 * @author Mozaffar Afaque
 */
public class IOLocation {
    private IOLocationType ioLocationType;
    private String fileSystemPath;
    private String objectName;

    private IOLocation() {

    }
    public static IOLocationBuilder newBuilder() {
        return new IOLocationBuilder();
    }

    public static class IOLocationBuilder {
        private IOLocationBuilder() {}

        private IOLocation ioLocation = new IOLocation();

        public IOLocationBuilder setIoType(IOLocationType ioLocationType) {
            ioLocation.ioLocationType = ioLocationType;
            return this;
        }

        public IOLocationBuilder setFileSystemPath(String fileSystemPath) {
            ioLocation.fileSystemPath = fileSystemPath;
            return this;
        }

        public IOLocationBuilder setObjectName(String objectName) {
            ioLocation.objectName = objectName;
            return this;
        }

        public IOLocation build() {
            Objects.requireNonNull(ioLocation.ioLocationType, "IO Type cannot be null");
            if (ioLocation.ioLocationType != IOLocationType.RECORD_STREAM) {
                Objects.requireNonNull(ioLocation.objectName, "Object name cannot be null");
            }
            IOLocation ioLocationTemp = ioLocation;
            ioLocation = new IOLocation();
            return ioLocationTemp;
        }
    }

    public IOLocationType getIoLocationType() {
        return ioLocationType;
    }

    public String getFileSystemPath() {
        return fileSystemPath;
    }

    public String getObjectName() {
        return objectName;
    }

    @Override
    public String toString() {
        return "IOLocation{" +
                "ioLocationType=" + ioLocationType +
                ", fileSystemPath='" + fileSystemPath + '\'' +
                ", objectName='" + objectName + '\'' +
                '}';
    }
}
