package com.mozafaq.extmergesort;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author Mozaffar Afaque
 */
public class ExecutionSummary {
    private Duration totalTimeTakenInMergeSort;
    private Duration readFromSourceTime;
    private Duration writeToDestinationTime;
    private Duration temporaryFilesWriteTime;
    private int noOfRecordsSorted;
    private List<String> outputFiles;
    private Duration splitTimeTakenIntoStagedFile;
    private Duration timeTakenInMergingAndWriting;

    private ExecutionSummary() {

    }

    public static ExecutionSummaryBuilder newBuilder() {
        return new ExecutionSummaryBuilder();
    }
    static class ExecutionSummaryBuilder {
        ExecutionSummary executionSummary = new ExecutionSummary();
        private ExecutionSummaryBuilder(){}

        public ExecutionSummaryBuilder setTotalTimeTakenInMergeSort(Duration totalTimeTakenInMergeSort) {
            executionSummary.totalTimeTakenInMergeSort = totalTimeTakenInMergeSort;
            return this;
        }

        public ExecutionSummaryBuilder setReadFromSourceTime(Duration readFromSourceTime) {
            executionSummary.readFromSourceTime = readFromSourceTime;
            return this;
        }

        public ExecutionSummaryBuilder setTemporaryFilesWriteTime(Duration temporaryFilesWriteTime) {
            executionSummary.temporaryFilesWriteTime = temporaryFilesWriteTime;
            return this;
        }

        public ExecutionSummaryBuilder setNoOfRecordsSorted(int noOfRecordsSorted) {
            executionSummary.noOfRecordsSorted = noOfRecordsSorted;
            return this;
        }

        public ExecutionSummaryBuilder setWriteToDestinationTime(Duration writeToDestinationTime) {
            executionSummary.writeToDestinationTime = writeToDestinationTime;
            return this;
        }

        public ExecutionSummaryBuilder setOutputFiles(List<String> outputFiles) {
            Objects.requireNonNull(outputFiles);
            executionSummary.outputFiles = Collections.unmodifiableList(outputFiles);
            return this;
        }

        public ExecutionSummaryBuilder setSplitTimeTakenIntoStagedFile(Duration splitTimeTakenIntoStagedFile) {
            executionSummary.splitTimeTakenIntoStagedFile = splitTimeTakenIntoStagedFile;
            return this;
        }


        public ExecutionSummaryBuilder setTimeTakenInMergingAndWriting(Duration timeTakenInMergingAndWriting) {
            executionSummary.timeTakenInMergingAndWriting = timeTakenInMergingAndWriting;
            return this;
        }

        public ExecutionSummary build() {
            return executionSummary;
        }
    }

    public Duration getTotalTimeTakenInMergeSort() {
        return totalTimeTakenInMergeSort;
    }

    public Duration getReadFromSourceTime() {
        return readFromSourceTime;
    }

    public Duration getTemporaryFilesWriteTime() {
        return temporaryFilesWriteTime;
    }

    public int getNoOfRecordsSorted() {
        return noOfRecordsSorted;
    }

    public Duration getWriteToDestinationTime() {
        return writeToDestinationTime;
    }

    public List<String> getOutputFiles() {
        return outputFiles;
    }

    public Duration getSplitTimeTakenIntoStagedFile() {
        return splitTimeTakenIntoStagedFile;
    }

    public Duration getTimeTakenInMergingAndWriting() {
        return timeTakenInMergingAndWriting;
    }

    @Override
    public String toString() {
        return "ExecutionSummary{" +
                "\ntotalTimeTakenInMergeSort=" + totalTimeTakenInMergeSort +
                ",\n readFromSourceTime=" + readFromSourceTime +
                ",\n writeToDestinationTime=" + writeToDestinationTime +
                ",\n temporaryFilesWriteTime=" + temporaryFilesWriteTime +
                ",\n splitTimeTakenIntoStagedFile=" + splitTimeTakenIntoStagedFile +
                ",\n timeTakenInMergingAndWriting=" + timeTakenInMergingAndWriting +
                ",\n noOfRecordsSorted=" + noOfRecordsSorted +
                ",\n outputFiles=" + outputFiles +
                '}';
    }
}
