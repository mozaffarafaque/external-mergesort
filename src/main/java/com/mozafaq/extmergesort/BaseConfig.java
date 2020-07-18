package com.mozafaq.extmergesort;

/**
 * @author Mozaffar Afaque
 */
public class BaseConfig {

    private int maxRecordInMemory;
    private String temporaryFileDirectory;
    private int maxRecordInOutputBatch;
    private SortAware sortAware;

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

        public BaseConfigBuilder setSortAware(SortAware sortAware) {
            baseConfig.sortAware = sortAware;
            return this;
        }

        public BaseConfig build() {
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

    public SortAware getSortAware() {
        return sortAware;
    }

    @Override
    public String toString() {
        return "BaseConfig{" +
                "maxRecordInMemory=" + maxRecordInMemory +
                ", temporaryFileDirectory='" + temporaryFileDirectory + '\'' +
                ", maxRecordInOutputBatch=" + maxRecordInOutputBatch +
                ", sortAware=" + sortAware +
                '}';
    }
}
