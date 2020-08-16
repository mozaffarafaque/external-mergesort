package com.mozafaq.extmergesort;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author Mozaffar Afaque
 */
class SortController<T> {

    private RecordReader<T> recordReader;
    final private RecordWriter<T> recordWriter;
    final private RecordWriter<T> resultWriter;
    final private Comparator<T> comparator;

    private long timeForReadingRecordsFromSource;
    private long timeFroWritingTemporaryFiles;

    public SortController(SortHandleProvider<T> sortHandleProvider) {
        Objects.requireNonNull(sortHandleProvider);
        Objects.requireNonNull(sortHandleProvider.recordReader());
        Objects.requireNonNull(sortHandleProvider.recordWriter());
        Objects.requireNonNull(sortHandleProvider.recordComparator());
        this.recordReader = sortHandleProvider.recordReader();
        this.recordWriter = sortHandleProvider.recordWriter();
        final StreamResultOutputHandler<T> streamResultOutputHandler = sortHandleProvider.streamResultOutputHandler();
        this.resultWriter = streamResultOutputHandler == null ? this.recordWriter : new RecordWriter<T>() {
            @Override
            public void onBegin() {
                streamResultOutputHandler.onBegin();
            }
            @Override
            public void onComplete() {
                streamResultOutputHandler.onComplete();
            }
            @Override
            public void writeRecord(OutputStream outputStream, T record) throws IOException {
                streamResultOutputHandler.accept(record);
            }
        };

        this.comparator = sortHandleProvider.recordComparator();
        this.timeForReadingRecordsFromSource = 0;
        this.timeFroWritingTemporaryFiles = 0;
    }

    /**
     * Sorts the records.
     */
    public ExecutionSummary sort(Configuration configuration) throws IOException {
        long startTime = System.currentTimeMillis();
        Objects.requireNonNull(configuration);

        List<String> temporaryFiles = splitIntoLocalFilesSorted(configuration.getBaseConfig(), configuration.getSource());
        ExecutionSummary executionSummary = sortKSortedFiles(configuration, temporaryFiles,  startTime);
        clearTemporaryFiles(temporaryFiles);
        return executionSummary;
    }

    ExecutionSummary sortKSortedFiles(Configuration configuration, List<String> files, long startTime) throws IOException {

        List<StreamReader> streamReaders =
                files.stream()
                       .map(e -> InputStreamFactory.getTempStreamReader(e))
                       .collect(Collectors.toList());

        long timeTakenToSplitFiles = System.currentTimeMillis() - startTime;

        int batchSizeForReadingFromInput =
                getRecordCountPerPartition(configuration.getBaseConfig().getMaxRecordInMemory(), streamReaders.size() + 1);

        long startMergingTime = System.currentTimeMillis();
        BatchReader<T> batchReader = new BatchReader<>(batchSizeForReadingFromInput);
        List<ComparisionIterator<T>> comparisionIterators = new ArrayList<>();
        for (StreamReader sr : streamReaders) {
            sr.open();
            comparisionIterators.add(batchReader.getStreamedIterator(sr.get(), recordReader));
        }

        PriorityQueue<ComparisionIterator<T>> priorityQueue =
                new PriorityQueue<>((e1, e2) -> comparator.compare(e1.current(), e2.current()));
        comparisionIterators.stream().forEach(e -> priorityQueue.add(e));

        ResultWriter<T> resultWriter = new ResultWriter<T>(batchSizeForReadingFromInput, configuration, this.resultWriter);

        int recordsWritten = 0;

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

        return ExecutionSummary.newBuilder()
                .setNoOfRecordsSorted(recordsWritten)
                .setWriteToDestinationTime(Duration.ofMillis(resultWriter.getTimeTakenToWriteInOutput()))
                .setTotalTimeTakenInMergeSort(Duration.ofMillis(System.currentTimeMillis() - startTime))
                .setTemporaryFilesWriteTime(Duration.ofMillis(timeFroWritingTemporaryFiles))
                .setReadFromSourceTime(Duration.ofMillis(timeForReadingRecordsFromSource))
                .setSplitTimeTakenIntoStagedFile(Duration.ofMillis(timeTakenToSplitFiles))
                .setOutputFiles(resultWriter.getOutFiles())
                .setTimeTakenInMergingAndWriting(Duration.ofMillis(System.currentTimeMillis() - startMergingTime))
                .setTemporaryFileCount(files.size())
                .build();
    }

    void clearTemporaryFiles(List<String> temporaryFiles) {
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

    List<String> splitIntoLocalFilesSorted(BaseConfig baseConfig, IOLocation sourceLocation) throws IOException {
        StreamReader sourceReader = InputStreamFactory.newStreamReader(sourceLocation);

        List<String> temporaryFiles ;
        final BatchReader<T> batchReader = new BatchReader<>(baseConfig.getMaxRecordInMemory());

        try(final InputStream sourceReaderStream = sourceReader.open()) {

            Supplier<List<T>> recordSupplier = () -> {
                try {
                    return batchReader.readFullBatch(sourceReaderStream, recordReader);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            };

            temporaryFiles = writeSortedFiles(baseConfig, recordSupplier);
        }

        return temporaryFiles;
    }

    /**
     * Collects the set of records in the temporary file.
     *
     */
    List<String> collect(BaseConfig baseConfig, List<T> records) throws IOException {

        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        Supplier<List<T>> recordSupplier = () -> {
            if (!atomicBoolean.get()) {
                atomicBoolean.set(true);
                return records;
            } else {
                return Collections.emptyList();
            }
        };
        return writeSortedFiles(baseConfig, recordSupplier);
    }

    private List<String> writeSortedFiles(BaseConfig baseConfig, Supplier<List<T>> recordSupplier) throws IOException {

        List<String> temporaryFiles = new ArrayList<>();
        long start = System.currentTimeMillis();
        List<T> batchRecords = recordSupplier.get();
        timeForReadingRecordsFromSource += (System.currentTimeMillis() - start);
        while (batchRecords.size() > 0) {
            start = System.currentTimeMillis();
            Collections.sort(batchRecords, comparator);
            try (StreamWriter writer = OutputStreamFactory.getTempStreamWriter(baseConfig.getTemporaryFileDirectory(), recordWriter)) {
                BatchReader.writeFullBatch(batchRecords.iterator(), writer.open(), recordWriter);
                temporaryFiles.add(writer.getFullPath());
            }
            batchRecords.clear();
            timeFroWritingTemporaryFiles += (System.currentTimeMillis() - start);

            start = System.currentTimeMillis();
            batchRecords = recordSupplier.get();
            timeForReadingRecordsFromSource += (System.currentTimeMillis() - start);
        }

        return temporaryFiles;
    }
}
