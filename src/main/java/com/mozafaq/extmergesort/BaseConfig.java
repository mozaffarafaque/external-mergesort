package com.mozafaq.extmergesort;

import java.io.File;
import java.util.Objects;

/**
 * @author Mozaffar Afaque
 */
public class BaseConfig {

    private int maxRecordInMemory;
    private String temporaryFileDirectory;
    private int maxRecordInOutputBatch;
    private SortHandleProvider sortHandleProvider;

    private BaseConfig() {
    }

    public static BaseConfigBuilder builder() {
        return new BaseConfigBuilder();
    }

    public static  class BaseConfigBuilder {
        BaseConfig baseConfig = new BaseConfig();
        private BaseConfigBuilder() {

        }

        public BaseConfigBuilder setMaxRecordInMemory(int maxRecordInMemory) {
            baseConfig.maxRecordInMemory = maxRecordInMemory;
            return this;
        }

        public BaseConfigBuilder setTemporaryFileDirectory(String temporaryFileDirectory) {
            baseConfig.temporaryFileDirectory = temporaryFileDirectory;
            return this;
        }

        public BaseConfigBuilder setMaxRecordInOutputBatch(int maxRecordInOutputBatch) {
            baseConfig.maxRecordInOutputBatch = maxRecordInOutputBatch;
            return this;
        }

        public BaseConfigBuilder setSortAware(SortHandleProvider sortHandleProvider) {
            baseConfig.sortHandleProvider = sortHandleProvider;
            return this;
        }

        public BaseConfig build() {
            Objects.requireNonNull(baseConfig.sortHandleProvider, "Sort Aware object cannot be null.");
            Objects.requireNonNull(baseConfig.temporaryFileDirectory , "Temp directory cannot be null.");
            if (!new File(baseConfig.temporaryFileDirectory).isDirectory()) {
                throw new IllegalArgumentException("Temp location must be directory");
            }
            BaseConfig baseConfigTemp = baseConfig;
            baseConfig = new BaseConfig();
            return baseConfigTemp;
        }
    }

    public int getMaxRecordInMemory() {
        return maxRecordInMemory;
    }

    public String getTemporaryFileDirectory() {
        return temporaryFileDirectory;
    }

    public int getMaxRecordInOutputBatch() {
        return maxRecordInOutputBatch;
    }

    public SortHandleProvider getSortHandleProvider() {
        return sortHandleProvider;
    }

    @Override
    public String toString() {
        return "BaseConfig{" +
                "maxRecordInMemory=" + maxRecordInMemory +
                ", temporaryFileDirectory='" + temporaryFileDirectory + '\'' +
                ", maxRecordInOutputBatch=" + maxRecordInOutputBatch +
                ", sortHandleProvider=" + sortHandleProvider +
                '}';
    }
}
