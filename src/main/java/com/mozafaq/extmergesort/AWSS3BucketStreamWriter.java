package com.mozafaq.extmergesort;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Mozaffar Afaque
 */

class AWSS3BucketStreamWriter implements StreamWriter {

    private boolean isClosed = false;
    private String path;
    private String objectName ;
    private String region;
    private BoundedStreamAware boundedStreamAware;

    final private StreamWriter tempStreamFileWriter;
    private AWSCredentials awsCredsAwsCredentials;
    private AmazonS3 s3client = null;

    public AWSS3BucketStreamWriter(IOLocation ioLocation, StreamWriter tempStreamFileWriter, BoundedStreamAware boundedStreamAware) {
        this.path = ioLocation.getS3Path();
        this.objectName = ioLocation.getObjectName();
        this.region = ioLocation.getS3Region();
        this.tempStreamFileWriter = tempStreamFileWriter;
        this.boundedStreamAware = boundedStreamAware;
    }

    public void create() {
        s3client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredsAwsCredentials))
                .build();
    }

    @Override
    public OutputStream open() throws IOException {

        OutputStream outputStream = tempStreamFileWriter.open();
        boundedStreamAware.onBegin();
        return outputStream;
    }

    @Override
    public OutputStream get() {
        return tempStreamFileWriter.get();
    }

    @Override
    public String getFullPath() {
        return String.format("s3://%s/%s", path, objectName);
    }

    private void transferData(String fullPath) throws IOException {

        InputStream is = new FileInputStream(fullPath);
        ObjectMetadata objectMetadata = new ObjectMetadata() ;
        PutObjectRequest request =
                new PutObjectRequest(path, objectName, is, objectMetadata);
        s3client.putObject(request);
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            boundedStreamAware.onComplete();
            transferData(tempStreamFileWriter.getFullPath());
            tempStreamFileWriter.close();
            Files.delete(Path.of(tempStreamFileWriter.getFullPath()));
            isClosed = true;
        }
    }

    public void setAwsCredentials(AWSCredentials awsCredsAwsCredentials) {
        this.awsCredsAwsCredentials = awsCredsAwsCredentials;
    }
}

