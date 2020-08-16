package com.mozafaq.extmergesort;

import java.io.IOException;
import java.util.Objects;

/**
 * Handler for performing external merge sort.
 *
 * @author Mozaffar Afaque
 */
public class ExternalMergeSort<T> {

    final private SortHandleProvider<T> sortHandleProvider;
    final private Configuration configuration;

    public ExternalMergeSort(SortHandleProvider<T> sortHandleProvider, Configuration configuration) {
        this.sortHandleProvider = sortHandleProvider;
        this.configuration = configuration;
    }

    public ExecutionSummary sort() throws IOException {
       return sort(this.sortHandleProvider, this.configuration);
    }

    public static <T> ExecutionSummary sort(SortHandleProvider<T> sortHandleProvider, Configuration configuration) throws IOException {
        Objects.requireNonNull(sortHandleProvider, "Sort aware cannot be null.");
        Objects.requireNonNull(configuration, "Configuration cannot be null.");
        return new SortController<T>(sortHandleProvider).sort(configuration);
    }
}
