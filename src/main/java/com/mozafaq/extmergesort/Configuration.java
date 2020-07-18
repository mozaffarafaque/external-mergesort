package com.mozafaq.extmergesort;

/**
 * @author Mozaffar Afaque
 */
public class Configuration {

    private IOLocation source;
    private IOLocation destination;
    private BaseConfig baseConfig;

    private Configuration() {}
    public static ConfigurationBuilder newBuilder() {
        return new ConfigurationBuilder();
    }

    public static class ConfigurationBuilder {

        private Configuration configuration = new Configuration();
        private ConfigurationBuilder() {

        }

        public ConfigurationBuilder setBaseConfig(BaseConfig baseConfig) {
            configuration.baseConfig = baseConfig;
            return this;
        }

        public ConfigurationBuilder setSource(IOLocation source) {
            configuration.source = source;
            return this;
        }

        public ConfigurationBuilder setDestination(IOLocation destination) {
            configuration.destination = destination;
            return this;
        }

        public Configuration build() {
            Configuration configurationTemp = configuration;
            configuration = new Configuration();
            return configurationTemp;
        }
    }

    public IOLocation getSource() {
        return source;
    }

    public IOLocation getDestination() {
        return destination;
    }

    public BaseConfig getBaseConfig() {
        return baseConfig;
    }

    @Override
    public String toString() {
        return "Configuration{" +
                "source=" + source +
                ", destination=" + destination +
                ", baseConfig=" + baseConfig +
                '}';
    }
}


