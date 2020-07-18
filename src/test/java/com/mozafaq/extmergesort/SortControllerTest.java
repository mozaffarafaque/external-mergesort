package com.mozafaq.extmergesort;

import org.testng.annotations.Test;

import java.io.*;
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

    public String writeFile () throws IOException {
       String fName =  UUID.randomUUID().toString();

        FileWriter fr = new FileWriter(TEMP_LOCATION + "/" + fName);

        List<Integer> ints = IntStream.range(1, 1001)
                .boxed().collect(Collectors.toList());
        Collections.shuffle(ints);
        for (Integer i: ints) {
            fr.write(String.valueOf(i));
            fr.write("\n");
        }

        fr.close();
        return fName;
    }

    @Test
    public void testMergeSort() throws IOException {
        String destinationFileName = UUID.randomUUID().toString();
        String inputFileName = writeFile();
        Configuration configuration = Configuration.newBuilder()
                .setBaseConfig(BaseConfig.builder()
                        .setMaxRecordInMemory(100)
                        .setMaxRecordInOutputBatch(0)
                        .setSortAware(new TestSortAware())
                        .setTemporaryFileDirectory(TEMP_LOCATION)
                        .build())
                .setDestination(
                        IOLocation.newBuilder()
                                .setFileSystemPath(TEMP_LOCATION)
                                .setObjectName(destinationFileName)
                                .setIoType(IOType.FILE_SYSTEM)
                                .build())
                .setSource(
                        IOLocation.newBuilder()
                                .setFileSystemPath(TEMP_LOCATION)
                                .setIoType(IOType.FILE_SYSTEM)
                                .setObjectName(inputFileName)
                                .build()
                )
                .build();

        ExecutionSummary executionSummary =
                new SortController<Integer>(configuration.getBaseConfig()
                        .getSortAware())
                        .sort(configuration);

        File file = new File(String.format("%s/%s", TEMP_LOCATION, executionSummary.getOutputFiles().get(0)));
        FileInputStream fis = new FileInputStream(file);
        byte[] data = new byte[(int) file.length()];
        fis.read(data);
        fis.close();
        String str = new String(data, "UTF-8");
        List<Integer> result = Arrays.stream(str.split("\n"))
                .filter(e -> !e.trim().isEmpty())
                .map(e -> Integer.parseInt(e))
                .collect(Collectors.toUnmodifiableList());


        List<Integer> ints = IntStream.range(1, 1001)
                .boxed().collect(Collectors.toList());
        assertEquals(result, ints);
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