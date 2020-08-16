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
            case AWS_S3_BUCKET:
                Objects.requireNonNull(location.getS3Path());
                Objects.requireNonNull(location.getObjectName());

                AWSS3BucketStreamReader awsS3BucketStreamReader =
                        new AWSS3BucketStreamReader(location);
                AWSCredentials awsCredentials =
                        AWSUtils.createAWSCredentials(AWSUtils.AWS_PROFILE_DEFAULT);
                awsS3BucketStreamReader.setAwsCredsAwsCredentials(awsCredentials);
                awsS3BucketStreamReader.create();
                return awsS3BucketStreamReader;
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
