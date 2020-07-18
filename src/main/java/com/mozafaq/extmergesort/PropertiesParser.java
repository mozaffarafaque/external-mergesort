package com.mozafaq.extmergesort;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * @author Mozaffar Afaque
 */
public class PropertiesParser {

    static final List<Pair<String, InputProperty>> PROPERTIES = Arrays.<Pair<String, InputProperty>>asList(
            new Pair<>("sort.max.records.in.memory", InputProperty.MAX_RECORDS_IN_MEMORY),
            new Pair<>("sort.temporary.files.directory", InputProperty.TEMPORARY_FILES_DIRECTORY),
            new Pair<>("sort.max.record.in.output.batch", InputProperty.MAX_RECORD_IN_OUTPUT_BATCH),
            new Pair<>("sort.source.io.type", InputProperty.SOURCE_IO_TYPE),
            new Pair<>("sort.source.s3.path", InputProperty.SOURCE_S3_PATH),
            new Pair<>("sort.source.s3.region", InputProperty.SOURCE_S3_REGION),
            new Pair<>("sort.source.filesystem.path", InputProperty.SOURCE_FILESYSTEM_PATH),
            new Pair<>("sort.source.object.name", InputProperty.SOURCE_OBJECT_NAME),
            new Pair<>("sort.destination.io.type", InputProperty.DESTINATION_IO_TYPE),
            new Pair<>("sort.destination.s3.path", InputProperty.DESTINATION_S3_PATH),
            new Pair<>("sort.destination.s3.region", InputProperty.DESTINATION_S3_REGION),
            new Pair<>("sort.destination.filesystem.path", InputProperty.DESTINATION_FILESYSTEM_PATH),
            new Pair<>("sort.destination.object.name", InputProperty.DESTINATION_OBJECT_NAME),
            new Pair<>("sort.client.logic.provider", InputProperty.CLIENT_LOGIC_PROVIDER)
    );

    public Configuration parseConfiguration(String inputFileName) throws IOException {
        Configuration.ConfigurationBuilder configurationBuilder = Configuration.newBuilder();
        IOLocation.IOLocationBuilder ioLocationBuilderSource = IOLocation.newBuilder();
        IOLocation.IOLocationBuilder ioLocationBuilderDest = IOLocation.newBuilder();
        BaseConfig.BaseConfigBuilder baseConfigBuilder = BaseConfig.builder();
        try (InputStream input = new FileInputStream(inputFileName)) {
            Properties prop = new Properties();
            // load a properties file
            prop.load(input);

            for (Pair<String, InputProperty> property: PROPERTIES ) {
               populateProperty(prop.getProperty(property.getKey()),
                       property.getValue(),
                       baseConfigBuilder,
                       ioLocationBuilderSource,
                       ioLocationBuilderDest);
            }

        }

        configurationBuilder.setDestination(ioLocationBuilderDest.build());
        configurationBuilder.setSource(ioLocationBuilderSource.build());
        configurationBuilder.setBaseConfig(baseConfigBuilder.build());
        return configurationBuilder.build();
    }

    private void populateProperty(String propertyValue,
                                  InputProperty inputProperty,
                                  BaseConfig.BaseConfigBuilder baseConfigBuilder,
                                  IOLocation.IOLocationBuilder ioLocationBuilderSource,
                                  IOLocation.IOLocationBuilder ioLocationBuilderDest) {
         switch (inputProperty) {
             case MAX_RECORDS_IN_MEMORY:
                 baseConfigBuilder.setMaxRecordInMemory(Integer.parseInt(propertyValue));
                 break;
             case TEMPORARY_FILES_DIRECTORY:
                 baseConfigBuilder.setTemporaryFileDirectory(propertyValue);
                 break;
             case MAX_RECORD_IN_OUTPUT_BATCH:
                 baseConfigBuilder.setMaxRecordInOutputBatch(Integer.parseInt(propertyValue));
                 break;
             case SOURCE_IO_TYPE:
                 ioLocationBuilderSource.setIoType(IOType.valueOf(propertyValue));
                 break;
             case SOURCE_S3_PATH:
                 ioLocationBuilderSource.setS3Path(propertyValue);
                 break;
             case SOURCE_FILESYSTEM_PATH:
                 ioLocationBuilderSource.setFileSystemPath(propertyValue);
                 break;
             case SOURCE_S3_REGION:
                 ioLocationBuilderSource.setS3Region(propertyValue);
                 break;
             case SOURCE_OBJECT_NAME:
                 ioLocationBuilderSource.setObjectName(propertyValue);
                 break;
             case DESTINATION_IO_TYPE:
                 ioLocationBuilderDest.setIoType(IOType.valueOf(propertyValue));
                 break;
             case DESTINATION_S3_PATH:
                 ioLocationBuilderDest.setS3Path(propertyValue);
                 break;
             case DESTINATION_S3_REGION:
                 ioLocationBuilderDest.setS3Region(propertyValue);
                 break;
             case DESTINATION_FILESYSTEM_PATH:
                 ioLocationBuilderDest.setFileSystemPath(propertyValue);
                 break;
             case DESTINATION_OBJECT_NAME:
                 ioLocationBuilderDest.setObjectName(propertyValue);
                 break;
             case CLIENT_LOGIC_PROVIDER:

                 Objects.requireNonNull(propertyValue);
                 try {
                     Class clazz = Class.forName(propertyValue);
                     Constructor<SortAware> constructor = clazz.getConstructor(new Class[]{});
                     SortAware sortAware = constructor.newInstance(new Object[]{});
                     baseConfigBuilder.setSortAware(sortAware);

                 } catch (ClassNotFoundException |
                         NoSuchMethodException |
                         IllegalAccessException |
                         InvocationTargetException |
                         InstantiationException ex) {
                     throw new IllegalArgumentException(ex);
                 }
                 break;
             default:
                 throw new IllegalArgumentException("Property type " + inputProperty + ", not handled.");
         }
    }

}
