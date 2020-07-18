package com.mozafaq.extmergesort;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

/**
 * @author Mozaffar Afaque
 */
class Batch<T> {
    private final int size;

    public Batch(int size) {
        this.size = size;
    }

    public List<T> readFullBatch(InputStream inputStream, RecordReader<T> reader) throws IOException {
        Objects.requireNonNull(reader);
        Objects.requireNonNull(inputStream);

        List<T> records = new ArrayList<>(size);
        int readRecord = 0;
        T record;
        while (readRecord < size && (record = reader.readRecord(inputStream)) != null) {
            readRecord++;
            records.add(record);
        }
        return records;
    }

    public static <T> void writeFullBatch(Iterator<T> iterator, OutputStream outputStream, RecordWriter<T> writer) throws IOException {
        Objects.requireNonNull(writer);
        Objects.requireNonNull(iterator);
        Objects.requireNonNull(outputStream);

        while (iterator.hasNext()) {
            writeSingleRecord(outputStream, writer, iterator.next());
        }
    }

    public static <T> void writeSingleRecord(OutputStream outputStream, RecordWriter<T> writer, T record) throws IOException {
        writer.writeRecord(outputStream, record);
    }

    public ComparisionIterator<T> getStreamedIterator(
            InputStream inputStream, RecordReader<T> recordReader)
    {

        return new ComparisionIterator<T>() {
            final InputStream stream  = inputStream;
            final RecordReader<T> reader = recordReader;
            boolean isDone = false;
            List<T> readBatch ;
            Iterator<T> itr ;
            T currentlyReadRecord;
            {
                currentlyReadRecord = resetIterator();
            }

            T resetIterator() {
                try {
                    if (isDone) {
                        return null;
                    }
                    readBatch = Batch.this.readFullBatch(inputStream, reader);
                    itr = readBatch.iterator();
                    T record = itr.hasNext() ? itr.next() : null;
                    isDone = readBatch.size() < Batch.this.size;
                    return record;

                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public T current() {
                return currentlyReadRecord;
            }

            @Override
            public boolean hasNext() {
                return currentlyReadRecord != null;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                T t = currentlyReadRecord;
                if(!itr.hasNext()){
                    currentlyReadRecord = resetIterator();
                } else if (itr.hasNext() ){
                    currentlyReadRecord = itr.next();
                }
                return t;
            }
        };

    }

}
