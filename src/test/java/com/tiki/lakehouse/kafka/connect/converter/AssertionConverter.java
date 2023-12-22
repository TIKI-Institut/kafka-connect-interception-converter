package com.tiki.lakehouse.kafka.connect.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

public final class AssertionConverter implements Converter {

    public Map<String, ?> configs;
    public boolean isKey;
    public Schema conversionSchema;


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configs = configs;
        this.isKey = isKey;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        this.conversionSchema = schema;
        return new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return null;
    }
}