package com.mozafaq.extmergesort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Mozaffar Afaque
 */
public class ResultWriter<T> {

    private static final String[] PREFIXES = {"000", "00", "0", ""};

    final private int bufferSize;
    final private int maxRecordPerOutputFile;
    final private RecordWriter<T> writer;
    final private boolean isSingleFile;
    final private Configuration configuration;

    private StreamWriter streamWriter;
    private int recordInCurrentStream;
    private int fileCounter;
    private List<T> buffer;

    private List<String> outFiles;

    private long timeTakenToWriteInOutput;

    public ResultWriter(int bufferSize, Configuration configuration, RecordWriter<T> writer) {
        this.bufferSize = bufferSize;
        this.maxRecordPerOutputFile = configuration.getBaseConfig().getMaxRecordInOutputBatch();
        this.configuration = configuration;
        isSingleFile = maxRecordPerOutputFile == 0;
        this.writer = writer;
        fileCounter = 0;
        buffer = new ArrayList<>(this.bufferSize);
        this.outFiles = new ArrayList<>();
    }

    private StreamWriter createWriter() {

        final IOLocation destinationLocation = configuration.getDestination();
        IOLocation ioLocation = IOLocation.newBuilder()
                .setFileSystemPath(destinationLocation.getFileSystemPath())
                .setIoType(destinationLocation.getIoType())
                .setObjectName(getObjectName())
                .setS3Path(destinationLocation.getS3Path())
                .setS3Region(destinationLocation.getS3Region())
                .build();

        outFiles.add(ioLocation.getObjectName());
        return OuputStreamFactory.newStreamWriter(ioLocation,
                configuration.getBaseConfig().getTemporaryFileDirectory());
    }

    private String getObjectName() {
        if(isSingleFile) {
           return configuration.getDestination().getObjectName() + "-sorted";
        }

        int digits = 0;
        int counter = fileCounter;
        do {
            counter /= 10;
            digits++;
        } while (counter > 0);

        if (digits > 4) {
            throw new IllegalStateException("There are too many output files - More than 10K files not supported");
        }

        int padding = digits - 1;
        return configuration.getDestination().getObjectName() + "-sorted-" + PREFIXES[padding] + fileCounter;
    }

    public void writeRecord(T record) throws IOException {

        if (buffer.size() == bufferSize) {
            clearBuffer();
            buffer.clear();
        }
        buffer.add(record);
    }

    private void resetStream() throws IOException {
        if (streamWriter == null) {
            streamWriter = createWriter();
            streamWriter.open();
            return ;
        }

        if (isSingleFile) {
            return;
        }
        if (recordInCurrentStream == maxRecordPerOutputFile) {
            streamWriter.close();
            streamWriter = createWriter();
            streamWriter.open();
            return ;
        }
    }

    private void clearBuffer() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        if (isSingleFile) {
            resetStream();
            recordInCurrentStream += buffer.size();
            Batch.writeFullBatch(buffer.iterator(), streamWriter.get(), writer);
            buffer.clear();
            return;
        }
        int counter = 0;
        while (counter < buffer.size()) {
            if (recordInCurrentStream == maxRecordPerOutputFile || streamWriter == null) {
                resetStream();
                recordInCurrentStream = 0;
                fileCounter++;
            }
            Batch.writeSingleRecord(streamWriter.get(), writer, buffer.get(counter));
            recordInCurrentStream++;
            counter++;
        }
        buffer.clear();
        timeTakenToWriteInOutput = timeTakenToWriteInOutput + (System.currentTimeMillis() - startTime);
    }

    public void writingComplete() throws IOException {
        clearBuffer();
        if (streamWriter.get() != null) {
            streamWriter.close();
        }
    }

    public long getTimeTakenToWriteInOutput() {
        return timeTakenToWriteInOutput;
    }

    public List<String> getOutFiles() {
        return outFiles;
    }
}
