package com.mozafaq.extmergesort;

import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

/**
 * @author Mozaffar Afaque
 */
class AWSS3BucketStreamReader implements StreamReader {

    private S3ObjectInputStream inputStream = null;
    private boolean isClosed = false;
    private String path;
    private String objectName ;
    private String region;

    private AWSCredentials awsCredsAwsCredentials;

    private AmazonS3 s3client = null;

    public AWSS3BucketStreamReader(IOLocation ioLocation) {
        this.path = ioLocation.getS3Path();
        this.objectName = ioLocation.getObjectName();
        this.region = ioLocation.getS3Region();
    }

    public void create() {
        s3client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredsAwsCredentials))
                .build();
    }


    @Override
    public InputStream open() throws IOException {
        GetObjectRequest request = new GetObjectRequest(path, objectName);
        S3Object s3Object = s3client.getObject(request);
        inputStream = s3Object.getObjectContent();
        return inputStream;
    }

    @Override
    public InputStream get() {
        return inputStream;
    }

    @Override
    public String getFullPath() {
        return String.format("s3://%s/%s", path, objectName);
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            inputStream.close();
            isClosed = true;
        }
    }

    public void setAwsCredsAwsCredentials(AWSCredentials awsCredsAwsCredentials) {
        this.awsCredsAwsCredentials = awsCredsAwsCredentials;
    }
}
