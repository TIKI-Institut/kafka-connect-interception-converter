package com.tiki.lakehouse.kafka.connect.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import java.util.List;
import java.util.Map;

public class InterceptionConverter implements Converter {

    public List<Interceptor> interceptors;

    public  Converter wrappedConverter;

    public InterceptionConverter() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        var myConfig = new InterceptionConverterConfig(configs);
        this.wrappedConverter = myConfig.wrappedConfiguredConverterInstance(isKey);
        this.interceptors = myConfig.interceptors();
    }

    @Override
    public byte[] fromConnectData(String s, Schema schema, Object o) {
        Schema result = schema;
        for (Interceptor interceptor : this.interceptors) {
            result = interceptor.Apply(result);
        }
        return this.wrappedConverter.fromConnectData(s, result, o);
    }

    @Override
    public SchemaAndValue toConnectData(String s, byte[] bytes) {

        var schemaAndValue = this.wrappedConverter.toConnectData(s, bytes);

        if (schemaAndValue == SchemaAndValue.NULL) {
            return schemaAndValue;
        }

        var schema = schemaAndValue.schema();
        for (Interceptor interceptor : this.interceptors) {
            interceptor.Apply(schema);
        }

        return new SchemaAndValue(schema, schemaAndValue.value());
    }

}
