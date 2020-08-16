package com.mozafaq.extmergesort;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.*;

/**
 * @author  Mozaffar Afaque
 */
public class SortControllerTest {

    static String TEMP_LOCATION = "/tmp";

    public static String getAbsolutePath(String path) {
        return SortControllerTest.class.getResource(path).getPath();
    }

    public String writeFile (int min, int max) throws IOException {
        final String fName = UUID.randomUUID().toString();
        FileWriter fr = new FileWriter(TEMP_LOCATION + "/" + fName);
        List<Integer> ints = IntStream.range(min, max + 1)
                .boxed().collect(Collectors.toList());
        Collections.shuffle(ints);
        for (Integer i : ints) {
            fr.write(String.valueOf(i));
            fr.write("\n");
        }

        fr.close();
        return fName;
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMergeInvalidDestinationIO() throws IOException {
        String destinationFileName = UUID.randomUUID().toString();
        String inputFileName = writeFile(1, 1);
        try {
            getTestConfiguration(destinationFileName,
                    inputFileName,
                    null,
                    new TestSortAware(), 100, 0);
        } finally {
            Files.delete(Path.of(TEMP_LOCATION, inputFileName));
        }
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMergeInvalidInLocation() throws IOException {
        String destinationFileName = UUID.randomUUID().toString();
        getTestConfiguration(destinationFileName,
                null,
                IOType.FILE_SYSTEM,
                new TestSortAware(), 100, 0);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMergeInvalidSortAware() throws IOException {
        String destinationFileName = UUID.randomUUID().toString();
        String fileName = writeFile(1, 1);
        try {
            getTestConfiguration(destinationFileName,
                    fileName,
                    IOType.FILE_SYSTEM,
                    null, 100, 0);
        } finally {
            Files.delete(Path.of(TEMP_LOCATION, fileName));
        }
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMergeMissingOutputStream() throws IOException {
        String destinationFileName = UUID.randomUUID().toString();
        String fileName = writeFile(1,1);
        try {
            getTestConfiguration(destinationFileName,
                    fileName,
                    IOType.RECORD_STREAM,
                    new TestSortAware(), 100, 0);
        } finally {
            Files.delete(Path.of(TEMP_LOCATION, fileName));
        }
    }

    static class ResultRecordStreamImpl implements ResultRecordStream<Integer> {
        private List<Integer> resultCollection = new ArrayList<>();
        private int beginCallCount = 0;
        private int completeCallCount = 0;
        @Override
        public void accept(Integer record) {
            resultCollection.add(record);
        }

        @Override
        public void onBegin() {
            beginCallCount++;
        }
        @Override
        public void onComplete() {
            completeCallCount++;
        }

        public List<Integer> getResultCollection() {
            return resultCollection;
        }

        public int getBeginCallCount() {
            return beginCallCount;
        }

        public int getCompleteCallCount() {
            return completeCallCount;
        }
    }

    @Test
    public void testMergeSortStreamOutput() throws IOException {
        String destinationFileName = UUID.randomUUID().toString();
        String inputFileName = writeFile(1, 953);

        final  ResultRecordStreamImpl resultRecordStream = new ResultRecordStreamImpl();
        SortAware<Integer> sortAware = new TestSortAware() {
          @Override
          public ResultRecordStream<Integer> resultRecordStream() {
              return resultRecordStream;
          }
        };
        Configuration configuration = getTestConfiguration(destinationFileName,
                inputFileName,
                IOType.RECORD_STREAM, sortAware, 100, 0);

        ExecutionSummary executionSummary =
                new SortController<Integer>(configuration.getBaseConfig().getSortAware())
                        .sort(configuration);

        assertTrue(executionSummary.getOutputFiles().isEmpty());

        List<Integer> ints = IntStream.range(1, 954)
                .boxed().collect(Collectors.toList());

        assertEquals(executionSummary.getNoOfRecordsSorted(), 953);
        assertEquals(resultRecordStream.getResultCollection(), ints);
        assertEquals(resultRecordStream.getBeginCallCount(), 1);
        assertEquals(resultRecordStream.getCompleteCallCount(), 1);
        Files.delete(Path.of(TEMP_LOCATION, inputFileName));
    }

    @DataProvider(name = "MergeSortTestCasesMaster")
    public Object[][] testGeneratedCases() throws IOException {
        List<Integer> recordsToBeSorted = Arrays.asList(
                10, 1000, 333, 1001, 99, 1001, 999, 553
        );
        List<Integer> maxRecordsInMemoryList = Arrays.asList(
                10, 1, 10000, 500
        );
        List<Integer> recordsPerOutputFileList = Arrays.asList(
                0, 100, 1000, 500
        );
        int testCaseCount = recordsToBeSorted.size() * maxRecordsInMemoryList.size() * recordsPerOutputFileList.size();
        Object[][] testCases = new Object[testCaseCount][3];

        int counter = 0;
        for (int records: recordsToBeSorted) {
            for (int maxRecordsInMemory : maxRecordsInMemoryList) {
                for (int recordsPerOutputFile : recordsPerOutputFileList) {
                    testCases[counter++] = new Object[] {records, maxRecordsInMemory, recordsPerOutputFile};
                }
            }
        }
        return testCases;
    }

    @Test(dataProvider = "MergeSortTestCasesMaster")
    public void testMergeSort(int records,
                              int maxRecordsInMemory,
                              int recordsPerOutputFile )
            throws IOException {

        System.out.println(
                String.format(
                        "Test running for records: %d, maxRecordsInMemory: %d, recordsPerOutputFile: %d ",
                        records, maxRecordsInMemory, recordsPerOutputFile)
        );
        String destinationFileName = UUID.randomUUID().toString();
        String inputFileName = writeFile(1, records);
        TestSortAware testSortAware = new TestSortAware();
        Configuration configuration = getTestConfiguration(destinationFileName,
                inputFileName,
                IOType.FILE_SYSTEM,
                testSortAware,
                maxRecordsInMemory,
                recordsPerOutputFile);

        ExecutionSummary executionSummary =
                new SortController<Integer>(configuration.getBaseConfig().getSortAware())
                        .sort(configuration);

        int noOfOutFiles = recordsPerOutputFile == 0 ? 1 :
                (records + (recordsPerOutputFile - 1)) / recordsPerOutputFile;
        assertEquals(executionSummary.getOutputFiles().size(), noOfOutFiles);

        for (int i = 0; i < noOfOutFiles; i++) {
            Path path = Path.of(TEMP_LOCATION , executionSummary.getOutputFiles().get(i));
            List<Integer> result = getFileContentAsParsedInts(new File(path.toString()));

            int startNumber = i * recordsPerOutputFile + 1;
            int endNumber = Math.min(records + 1,
                    (i + 1) * (recordsPerOutputFile == 0 ? records : recordsPerOutputFile) + 1);
            List<Integer> ints = IntStream.range(startNumber, endNumber)
                    .boxed().collect(Collectors.toList());
            assertEquals(result, ints, "File name: " + path);
            Files.delete(path);
        }

        int beginCallsOnResult = testSortAware.getOnBeginCallCount().get() - executionSummary.getTemporaryFileCount();
        int completeCallsOnResult = testSortAware.getOnCompleteCallCount().get() - executionSummary.getTemporaryFileCount();
        assertEquals(beginCallsOnResult, noOfOutFiles);
        assertEquals(completeCallsOnResult, noOfOutFiles);

        assertEquals(testSortAware.getOnBeginCallCount().get() , noOfOutFiles + executionSummary.getTemporaryFileCount());
        assertEquals(testSortAware.getOnCompleteCallCount().get() , noOfOutFiles + executionSummary.getTemporaryFileCount());

        assertEquals(executionSummary.getTemporaryFileCount(), (records + maxRecordsInMemory - 1)/ maxRecordsInMemory);

        assertEquals(executionSummary.getNoOfRecordsSorted(), records);
        assertEquals(executionSummary.getNoOfRecordsSorted(), records);
        assertNotNull(executionSummary.getWriteToDestinationTime());
        assertNotNull(executionSummary.getReadFromSourceTime());
        assertNotNull(executionSummary.getSplitTimeTakenIntoStagedFile());
        assertNotNull(executionSummary.getTimeTakenInMergingAndWriting());
        assertNotNull(executionSummary.getWriteToDestinationTime());
        assertNotNull(executionSummary.getTotalTimeTakenInMergeSort());
        assertNotNull(executionSummary.getTemporaryFilesWriteTime());
        assertTrue(executionSummary.getTemporaryFileCount() > 0,
                "Actual temporary file count: " + executionSummary.getTemporaryFileCount());
        Files.delete(Path.of(TEMP_LOCATION, inputFileName));
    }

    private List<Integer> getFileContentAsParsedInts(File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        byte[] data = new byte[(int) file.length()];
        fis.read(data);
        fis.close();
        String str = new String(data, "UTF-8");
        return Arrays.stream(str.split("\n"))
                .filter(e -> !e.trim().isEmpty())
                .map(e -> Integer.parseInt(e))
                .collect(Collectors.toUnmodifiableList());
    }

    private Configuration getTestConfiguration(String destinationFileName,
                                               String inputFileName,
                                               IOType destinationType,
                                               SortAware<Integer> sortAware,
                                               int mxRecordsInMemory,
                                               int recordPerOutputFile) {
        return Configuration.newBuilder()
                    .setBaseConfig(BaseConfig.builder()
                            .setMaxRecordInMemory(mxRecordsInMemory)
                            .setMaxRecordInOutputBatch(recordPerOutputFile)
                            .setSortAware(sortAware)
                            .setTemporaryFileDirectory(TEMP_LOCATION)
                            .build())
                    .setDestination(
                            IOLocation.newBuilder()
                                    .setFileSystemPath(TEMP_LOCATION)
                                    .setObjectName(destinationFileName)
                                    .setIoType(destinationType)
                                    .build())
                    .setSource(
                            IOLocation.newBuilder()
                                    .setFileSystemPath(TEMP_LOCATION)
                                    .setIoType(IOType.FILE_SYSTEM)
                                    .setObjectName(inputFileName)
                                    .build()
                    )
                    .build();
    }

    @Test
    public void propertyParserTest() throws IOException {
        String path = getAbsolutePath("/input.properties");
        //FileReader fr = new FileReader(path);
        Configuration configuration = new PropertiesParser().parseConfiguration(path);

        assertNotNull(configuration);
        assertNotNull(configuration.getSource());
        assertNotNull(configuration.getDestination());
        assertNotNull(configuration.getBaseConfig());
        assertEquals(configuration.getBaseConfig().getTemporaryFileDirectory(), TEMP_LOCATION);
        assertEquals(configuration.getBaseConfig().getMaxRecordInMemory(), 10);
        assertEquals(configuration.getBaseConfig().getMaxRecordInOutputBatch(), 0);
        assertNotNull(configuration.getBaseConfig().getSortAware());
        assertTrue(configuration.getBaseConfig().getSortAware() instanceof SortAware);
    }
}