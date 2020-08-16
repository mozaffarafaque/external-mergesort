package com.mozafaq.extmergesort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class provides ability ability to sort if input are coming as stream of records.
 * Create object if sorting needs to be done by providing stream of inputs.
 *
 * @author Mozaffar Afaque
 */
public class StreamInputSortController<T> {

    final private SortHandleProvider<T> sortHandleProvider;
    final private Configuration configuration;

    final private SortController<T> sortController;
    final private int maxBuffer;
    final private List<T> buffer;
    final private List<String> filesCollected;
    private transient boolean isCompleted;

    private long startTime = -1l;

    public StreamInputSortController(SortHandleProvider<T> sortHandleProvider, Configuration configuration) {
        this.sortHandleProvider = sortHandleProvider;
        this.configuration = configuration;
        sortController = new SortController<>(sortHandleProvider);
        maxBuffer = configuration.getBaseConfig().getMaxRecordInMemory();
        buffer = new ArrayList<>(maxBuffer);
        filesCollected = new ArrayList<>();
        isCompleted = false;
    }

    /**
     *
     * Consumes the records one by one to be sorted. Once all the records are inserted
     * then onComplete should be call for sorting and spit the output.
     *
     * @param record Next record to be sorted.
     *
     * @throws IOException if not able to write into temporary storage.
     */
    public void input(T record) throws IOException {
        if (startTime == -1l) {
            startTime = System.currentTimeMillis();
        }
        if (isCompleted) {
            throw new IllegalStateException("It is already completed, cannot take moe cords now!");
        }
        if (buffer.size() == maxBuffer) {
            filesCollected.addAll(sortController.collect(configuration.getBaseConfig(), buffer));
            buffer.clear();
        }
        buffer.add(record);
    }

    /**
     * After consuming all the records this operation sort the
     * records and sends the result to expected output location.
     *
     * @return Summary of sorting operation.
     *
     * @throws IOException In case of reading the files that has been used for storing
     * intermediate files to keep records off heap.
     */
    public ExecutionSummary onCompleted() throws IOException {
        if (isCompleted) {
            throw new IllegalStateException("On complete can be called only once.");
        }
        isCompleted = true;
        filesCollected.addAll(sortController.collect(configuration.getBaseConfig(), buffer));
        buffer.clear();
        startTime = startTime == -1l ? System.currentTimeMillis() : startTime;
        ExecutionSummary executionSummary =
                sortController.sortKSortedFiles(configuration, filesCollected, startTime);
        sortController.clearTemporaryFiles(filesCollected);
        return executionSummary;
    }
}
