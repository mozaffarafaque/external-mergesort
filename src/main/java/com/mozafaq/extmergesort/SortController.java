package com.mozafaq.extmergesort;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Mozaffar Afaque
 */
public class SortController<T> {

    private RecordReader<T> recordReader;
    private RecordWriter<T> recordWriter;
    private Comparator<T> comparator;

    private long timeForReadingRecordsFromSource;
    private long timeFroWritingTemporaryFiles;

    public SortController(SortAware<T> sortAware) {
        Objects.requireNonNull(sortAware);
        Objects.requireNonNull(sortAware.recordReader());
        Objects.requireNonNull(sortAware.recordWriter());
        Objects.requireNonNull(sortAware.recordComparator());
        this.recordReader = sortAware.recordReader();
        this.recordWriter = sortAware.recordWriter();
        this.comparator = sortAware.recordComparator();
        this.timeForReadingRecordsFromSource = 0;
        this.timeFroWritingTemporaryFiles = 0;
    }

    public ExecutionSummary sort(Configuration configuration) throws IOException {

        long startTime = System.currentTimeMillis();
        Objects.requireNonNull(configuration);

        List<String> temporaryFiles = splitIntoLocalFilesSorted(configuration.getBaseConfig(),
                configuration.getSource());

        List<StreamReader> streamReaders =
               temporaryFiles.stream()
                       .map(e -> InputStreamFactory.getTempStreamReader(e))
                       .collect(Collectors.toList());

        long timeTakenToSplitFiles = System.currentTimeMillis() - startTime;

        int batchSizeForReadingFromInput =
                getRecordCountPerPartition(configuration.getBaseConfig().getMaxRecordInMemory(), streamReaders.size() + 1);

        long startMergingTime = System.currentTimeMillis();
        Batch<T> batchReader = new Batch<>(batchSizeForReadingFromInput);
        List<ComparisionIterator<T>> comparisionIterators = new ArrayList<>();
        for (StreamReader sr : streamReaders) {
            sr.open();
            comparisionIterators.add(batchReader.getStreamedIterator(sr.get(), recordReader));
        }

        PriorityQueue<ComparisionIterator<T>> priorityQueue =
                new PriorityQueue<>((e1, e2) -> comparator.compare(e1.current(), e2.current()));
        comparisionIterators.stream().forEach(e -> priorityQueue.add(e));

        ResultWriter<T> resultWriter = new ResultWriter<T>(batchSizeForReadingFromInput, configuration, recordWriter);

        int recordsWritten = 0;
        long heapAdjustmentStartTime = System.nanoTime();

        while (!priorityQueue.isEmpty()) {

            ComparisionIterator<T>  comparisionIterator = priorityQueue.poll();
            if (comparisionIterator.hasNext()) {
              T record = comparisionIterator.next();
              resultWriter.writeRecord(record);
              recordsWritten++;
            }
            if (comparisionIterator.hasNext()) {
                priorityQueue.add(comparisionIterator);
            }
        }

        resultWriter.writingComplete();
        clearTemporaryFiles(temporaryFiles);

        return ExecutionSummary.newBuilder()
                .setNoOfRecordsSorted(recordsWritten)
                .setWriteToDestinationTime(Duration.ofMillis(resultWriter.getTimeTakenToWriteInOutput()))
                .setTotalTimeTakenInMergeSort(Duration.ofMillis(System.currentTimeMillis() - startTime))
                .setTemporaryFilesWriteTime(Duration.ofMillis(timeFroWritingTemporaryFiles))
                .setReadFromSourceTime(Duration.ofMillis(timeForReadingRecordsFromSource))
                .setSplitTimeTakenIntoStagedFile(Duration.ofMillis(timeTakenToSplitFiles))
                .setOutputFiles(resultWriter.getOutFiles())
                .setTimeTakenInMergingAndWriting(Duration.ofMillis(System.currentTimeMillis() - startMergingTime))
                .build();
    }

    private void clearTemporaryFiles(List<String> temporaryFiles) {
        for (String tempFile: temporaryFiles) {
            try {
                Files.delete(Path.of(tempFile));
            } catch (IOException ex) {
                // Suppress Exception
            }
        }
    }

    private int getRecordCountPerPartition(int maxRecordInMemory, int totalMergeBatches) {
        int batchSizeForReadingFromInput = maxRecordInMemory/ totalMergeBatches;
        batchSizeForReadingFromInput = Math.max(1, batchSizeForReadingFromInput);
        return batchSizeForReadingFromInput;
    }

    public List<String> splitIntoLocalFilesSorted(BaseConfig baseConfig, IOLocation sourceLocation) throws IOException {
        StreamReader sourceReader =
                InputStreamFactory.newStreamReader(sourceLocation);

        List<String> temporaryFiles = new ArrayList<>();
        Batch<T> batch = new Batch<>(baseConfig.getMaxRecordInMemory());

        try(InputStream sourceReaderStream = sourceReader.open()) {
            long start = System.currentTimeMillis();
            List<T> batchRecords = batch.readFullBatch(sourceReaderStream, recordReader);
            timeForReadingRecordsFromSource += (System.currentTimeMillis() - start);
            while (batchRecords.size() > 0) {
                start = System.currentTimeMillis();
                Collections.sort(batchRecords, comparator);
                StreamWriter writer =
                       OuputStreamFactory.getTempStreamWriter(baseConfig.getTemporaryFileDirectory());
                try (OutputStream outStream = writer.open()) {
                   Batch.writeFullBatch(batchRecords.iterator(), outStream, recordWriter);
                   temporaryFiles.add(writer.getFullPath());
                }
                batchRecords.clear();
                timeFroWritingTemporaryFiles += (System.currentTimeMillis() - start);

                start = System.currentTimeMillis();
                batchRecords = batch.readFullBatch(sourceReaderStream, recordReader);
                timeForReadingRecordsFromSource += (System.currentTimeMillis() - start);
            }
        }

        return temporaryFiles;
    }
}
