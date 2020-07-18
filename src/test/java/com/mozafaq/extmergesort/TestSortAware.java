package com.mozafaq.extmergesort;

import java.util.Comparator;

/**
 * @author Mozaffar Afaque
 */
public class TestSortAware implements SortAware<Integer> {
    @Override
    public RecordWriter<Integer> recordWriter() {
        return (outputStream, record) -> {
            char[] bytes = record.toString().toCharArray();

            for (char ch: bytes) {
                outputStream.write((byte)ch);
            }
            outputStream.write((int)'\n');
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
}
