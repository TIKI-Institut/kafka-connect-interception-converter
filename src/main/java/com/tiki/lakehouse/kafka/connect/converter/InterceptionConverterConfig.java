package com.tiki.lakehouse.kafka.connect.converter;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import org.apache.kafka.connect.storage.Converter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InterceptionConverterConfig extends AbstractConfig {
    public InterceptionConverterConfig(Map<?, ?> props) {
        super(baseConfigDef(), props);
    }

    public Converter wrappedConfiguredConverterInstance(Boolean isKey) {
        var result = this.getConfiguredInstance(WRAPPED_CONVERTER_CLASS_CONFIG, Converter.class);
        result.configure(this.originalsWithPrefix(WRAPPED_CONVERTER_CLASS_CONFIG_PREFIX), isKey);
        return result;
    }

    public List<Interceptor> interceptors() {
        return this.getConfiguredInstances(WRAPPED_CONVERTER_INTERCEPTORS_CONFIG, Interceptor.class);
    }

    public static final String WRAPPED_CONVERTER_CLASS_CONFIG = "wrapped.class";
    public static final String WRAPPED_CONVERTER_CLASS_CONFIG_DOC =
            "The wrapped instance which shall be used for conversion.";

    public static final String WRAPPED_CONVERTER_CLASS_CONFIG_PREFIX = "wrapped.config.";

    public static final String WRAPPED_CONVERTER_INTERCEPTORS_CONFIG = "interceptors";
    public static final String WRAPPED_CONVERTER_INTERCEPTORS_CONFIG_DOC =
            "Comma-separated list of class names used as interceptors.";


    public static ConfigDef baseConfigDef() {
        var configDef = new ConfigDef()
                .define(WRAPPED_CONVERTER_CLASS_CONFIG, ConfigDef.Type.CLASS, ConfigDef.Importance.HIGH, WRAPPED_CONVERTER_CLASS_CONFIG_DOC)
                .define(WRAPPED_CONVERTER_INTERCEPTORS_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.LOW, WRAPPED_CONVERTER_INTERCEPTORS_CONFIG_DOC);

        return configDef;
    }

}
