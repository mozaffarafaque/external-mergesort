package com.mozafaq.extmergesort;

import com.amazonaws.auth.AWSCredentials;

import java.util.Objects;
import java.util.UUID;

/**
 * @author Mozaffar Afaque
 */
public class OuputStreamFactory {

    public static StreamWriter newStreamWriter(IOLocation location, String tempLocation) {
        Objects.requireNonNull(location);
        Objects.requireNonNull(location.getIoType());

        switch (location.getIoType()) {
            case AWS_S3_BUCKET:
                Objects.requireNonNull(location.getS3Path());
                Objects.requireNonNull(location.getObjectName());

                StreamWriter streamFileWriter = getTempStreamWriter(tempLocation);
                AWSS3BucketStreamWriter awsS3BucketStreamWriter =
                        new AWSS3BucketStreamWriter(location, streamFileWriter);
                AWSCredentials awsCredentials =
                        AWSUtils.createAWSCredentials(AWSUtils.AWS_PROFILE_DEFAULT);
                awsS3BucketStreamWriter.setAwsCredentials(awsCredentials);
                awsS3BucketStreamWriter.create();
                return awsS3BucketStreamWriter;
            case FILE_SYSTEM:
                Objects.requireNonNull(location.getFileSystemPath());
                Objects.requireNonNull(location.getObjectName());
                String fileLocation = String.format("%s/%s", location.getFileSystemPath(), location.getObjectName());
                return new FileSystemStreamWriter(fileLocation);
        }

        throw new IllegalStateException(location.getIoType() + " case is not implemented");
    }

    public static StreamWriter getTempStreamWriter(String path) {
        Objects.requireNonNull(path);
        String objectName = UUID.randomUUID().toString() + ".tmp";
        return new FileSystemStreamWriter(String.format("%s/%s", path, objectName));
    }

}
