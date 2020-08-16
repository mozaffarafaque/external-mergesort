package com.mozafaq.extmergesort;

import java.util.Objects;

/**
 * @author Mozaffar Afaque
 */
public class IOLocation {
    private IOType ioType;
    private String s3Path;
    private String s3Region;
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

        public IOLocationBuilder setIoType(IOType ioType) {
            ioLocation.ioType = ioType;
            return this;
        }

        public IOLocationBuilder setS3Path(String s3Path) {
            ioLocation.s3Path = s3Path;
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

        public IOLocationBuilder setS3Region(String s3Region) {
            ioLocation.s3Region = s3Region;
            return this;
        }

        public IOLocation build() {
            Objects.requireNonNull(ioLocation.ioType, "IO Type cannot be null");
            if (ioLocation.ioType != IOType.RECORD_STREAM) {
                Objects.requireNonNull(ioLocation.objectName, "Object name cannot be null");
            }
            IOLocation ioLocationTemp = ioLocation;
            ioLocation = new IOLocation();
            return ioLocationTemp;
        }
    }

    public IOType getIoType() {
        return ioType;
    }

    public String getS3Path() {
        return s3Path;
    }

    public String getFileSystemPath() {
        return fileSystemPath;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getS3Region() {
        return s3Region;
    }


    @Override
    public String toString() {
        return "IOLocation{" +
                "ioType=" + ioType +
                ", s3Path='" + s3Path + '\'' +
                ", s3Region='" + s3Region + '\'' +
                ", fileSystemPath='" + fileSystemPath + '\'' +
                ", objectName='" + objectName + '\'' +
                '}';
    }
}
