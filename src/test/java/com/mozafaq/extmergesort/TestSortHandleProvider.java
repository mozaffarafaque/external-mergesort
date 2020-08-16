package com.mozafaq.extmergesort;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mozaffar Afaque
 */
public class TestSortHandleProvider implements SortHandleProvider<Integer> {

    private AtomicInteger onBeginCallCount = new AtomicInteger();
    private AtomicInteger onCompleteCallCount = new AtomicInteger();

    @Override
    public RecordWriter<Integer> recordWriter() {
        return new RecordWriter<Integer>() {
            @Override
            public void writeRecord(OutputStream outputStream, Integer record) throws IOException {
                char[] bytes = record.toString().toCharArray();

                for (char ch: bytes) {
                    outputStream.write((byte)ch);
                }
                outputStream.write((int)'\n');
            }

            @Override
            public void onBegin() {
                onBeginCallCount.incrementAndGet();
            }

            @Override
            public void onComplete() {
                onCompleteCallCount.incrementAndGet();
            }
        };
    }

    @Override
    public RecordReader<Integer> recordReader() {
        return inputStream -> {
            int ch;
            StringBuilder sb = new StringBuilder();
            while (inputStream.available() > 0 && (ch = inputStream.read()) != '\n' ) {
                sb.append((char)ch);
            }
            if(sb.length() > 0) {
                return Integer.parseInt(sb.toString());
            }
            return null;
        };
    }

    @Override
    public Comparator<Integer> recordComparator() {
        return ((o1, o2) -> o1 -o2);
    }

    @Override
    public StreamResultOutputHandler<Integer> streamResultOutputHandler() {
        return null;
    }

    public AtomicInteger getOnBeginCallCount() {
        return onBeginCallCount;
    }

    public AtomicInteger getOnCompleteCallCount() {
        return onCompleteCallCount;
    }
}