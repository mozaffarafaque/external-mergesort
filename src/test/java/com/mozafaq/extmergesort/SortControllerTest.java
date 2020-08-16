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
                    IOLocationType.FILE_SYSTEM,
                    null,
                    new TestSortHandleProvider(),
                    100,
                    0);
        } finally {
            Files.delete(Path.of(TEMP_LOCATION, inputFileName));
        }
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMergeInvalidInLocation() throws IOException {
        String destinationFileName = UUID.randomUUID().toString();
        getTestConfiguration(destinationFileName,
                null,
                IOLocationType.FILE_SYSTEM,
                IOLocationType.FILE_SYSTEM,
                new TestSortHandleProvider(),
                100,
                0);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMergeInvalidSortAware() throws IOException {
        String destinationFileName = UUID.randomUUID().toString();
        String fileName = writeFile(1, 1);
        try {
            getTestConfiguration(destinationFileName,
                    fileName,
                    IOLocationType.FILE_SYSTEM,
                    IOLocationType.FILE_SYSTEM,
                    null,
                    100,
                    0);
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
                    IOLocationType.FILE_SYSTEM,
                    IOLocationType.RECORD_STREAM,
                    new TestSortHandleProvider(),
                    100,
                    0);
        } finally {
            Files.delete(Path.of(TEMP_LOCATION, fileName));
        }
    }

    static class StreamResultOutputHandlerImpl implements StreamResultOutputHandler<Integer> {
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

        final StreamResultOutputHandlerImpl resultRecordStream = new StreamResultOutputHandlerImpl();
        SortHandleProvider<Integer> sortHandleProvider = new TestSortHandleProvider() {
          @Override
          public StreamResultOutputHandler<Integer> streamResultOutputHandler() {
              return resultRecordStream;
          }
        };
        Configuration configuration = getTestConfiguration(destinationFileName,
                inputFileName,
                IOLocationType.FILE_SYSTEM,
                IOLocationType.RECORD_STREAM,
                sortHandleProvider,
                100,
                0);

        ExecutionSummary executionSummary = ExternalMergeSort.sort(
                configuration.getBaseConfig().getSortHandleProvider(), configuration);

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
                1000, 1001, 99, 1001, 999, 553
        );
        List<Integer> maxRecordsInMemoryList = Arrays.asList(
                10, 500, 1000
        );
        List<Integer> recordsPerOutputFileList = Arrays.asList(
                0, 100, 1000, 500
        );
        int testCaseCount = recordsToBeSorted.size() * maxRecordsInMemoryList.size() * recordsPerOutputFileList.size() + 4;
        Object[][] testCases = new Object[testCaseCount][4];

        int counter = 0;
        for (int records: recordsToBeSorted) {
            for (int maxRecordsInMemory : maxRecordsInMemoryList) {
                for (int recordsPerOutputFile : recordsPerOutputFileList) {
                    testCases[counter++] = new Object[] {records, maxRecordsInMemory, recordsPerOutputFile, false};
                }
            }
        }
        testCases[counter++] = new Object[] {1000, 100, 100, true};
        testCases[counter++] = new Object[] {100, 99, 100, true};
        testCases[counter++] = new Object[] {10010, 1000, 100, true};
        testCases[counter++] = new Object[] {500, 499, 100, true};

        return testCases;
    }

    @Test(dataProvider = "MergeSortTestCasesMaster")
    public void testMergeSort(int records,
                              int maxRecordsInMemory,
                              int recordsPerOutputFile,
                              final boolean testWithStreamInputAndOutput)
            throws IOException {

        String destinationFileName = UUID.randomUUID().toString();
        String inputFileName = writeFile(1, records);
        final StreamResultOutputHandlerImpl resultRecordStream = new StreamResultOutputHandlerImpl();
        TestSortHandleProvider testSortAware = new TestSortHandleProvider() {
            @Override
            public StreamResultOutputHandler<Integer> streamResultOutputHandler() {
                return testWithStreamInputAndOutput ? resultRecordStream : null;
            }
        };

        IOLocationType destIOLocationType = !testWithStreamInputAndOutput ? IOLocationType.FILE_SYSTEM : IOLocationType.RECORD_STREAM;
        Configuration configuration = getTestConfiguration(destinationFileName,
                inputFileName,
                destIOLocationType,
                destIOLocationType,
                testSortAware,
                maxRecordsInMemory,
                recordsPerOutputFile);

        ExecutionSummary executionSummary = null;
        List<Integer> streamedInputValues = new ArrayList<>();
        if (!testWithStreamInputAndOutput) {
          executionSummary = ExternalMergeSort.sort(configuration.getBaseConfig().getSortHandleProvider(), configuration);
        }  else {
            StreamInputSortController streamInputSortController =
                    new StreamInputSortController(testSortAware, configuration);
             List<Integer> inputRecords = getFileContentAsParsedInts(
                    new File(Path.of(TEMP_LOCATION, inputFileName).toString()));
            for (Integer input: inputRecords) {
                streamInputSortController.input(input);
            }
            executionSummary = streamInputSortController.onCompleted();
            streamedInputValues.addAll(inputRecords);
        }

        int noOfOutFiles = getNoOfOutFiles(records, recordsPerOutputFile, testWithStreamInputAndOutput);
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

        Collections.sort(streamedInputValues);
        int beginCallsOnResult =
                testSortAware.getOnBeginCallCount().get() - executionSummary.getTemporaryFileCount();
        int completeCallsOnResult =
                testSortAware.getOnCompleteCallCount().get() - executionSummary.getTemporaryFileCount();

        assertEquals(resultRecordStream.getResultCollection(), streamedInputValues);
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

    private int getNoOfOutFiles(int records, int recordsPerOutputFile, boolean testWithStreamInputAndOutput) {
        if (testWithStreamInputAndOutput) {
            return 0;
        }
        return recordsPerOutputFile == 0 ? 1 :
                    (records + (recordsPerOutputFile - 1)) / recordsPerOutputFile;
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
                                               IOLocationType sourceType,
                                               IOLocationType destinationType,
                                               SortHandleProvider<Integer> sortHandleProvider,
                                               int mxRecordsInMemory,
                                               int recordPerOutputFile) {
        return Configuration.newBuilder()
                    .setBaseConfig(BaseConfig.builder()
                            .setMaxRecordInMemory(mxRecordsInMemory)
                            .setMaxRecordInOutputBatch(recordPerOutputFile)
                            .setSortAware(sortHandleProvider)
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
                                    .setIoType(sourceType)
                                    .setObjectName(inputFileName)
                                    .build()
                    )
                    .build();
    }

    @Test
    public void propertyParserTest() throws IOException {
        String path = getAbsolutePath("/input.properties");
        Configuration configuration = new PropertiesParser().parseConfiguration(path);

        assertNotNull(configuration);
        assertNotNull(configuration.getSource());
        assertNotNull(configuration.getDestination());
        assertNotNull(configuration.getBaseConfig());
        assertEquals(configuration.getBaseConfig().getTemporaryFileDirectory(), TEMP_LOCATION);
        assertEquals(configuration.getBaseConfig().getMaxRecordInMemory(), 10);
        assertEquals(configuration.getBaseConfig().getMaxRecordInOutputBatch(), 0);
        assertNotNull(configuration.getBaseConfig().getSortHandleProvider());
        assertTrue(configuration.getBaseConfig().getSortHandleProvider() instanceof SortHandleProvider);
    }
}