package com.tiki.lakehouse.kafka.connect.converter;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class InterceptionConverterConfigTest {


    @Test
    public void testWithMissingWrappedConfig() {

        assertThrows(
                "Missing required configuration \"wrapped.class\" which has no default value.",
                ConfigException.class,
                () -> {
                    var emptyConfig = Collections.emptyMap();
                    var converterConfig = new InterceptionConverterConfig(emptyConfig);
                });

    }

    @Test
    public void testExistingWrappedConfig() {
        var emptyConfig = Collections.singletonMap("wrapped.class", "com.tiki.lakehouse.kafka.connect.converter.InterceptionConverterConfigTest");
        var converterConfig = new InterceptionConverterConfig(emptyConfig);
        assertNotNull(converterConfig);
    }

    @Test
    public void testForwardConfigurationToWrappedConverterInstance() {
        Map<String,String> configs = new HashMap<>();

        configs.put("wrapped.class", "com.tiki.lakehouse.kafka.connect.converter.AssertionConverter");
        configs.put("wrapped.config.propA", "foo");
        configs.put("wrapped.config.propB", "bar");

        var converterConfig = new InterceptionConverterConfig(configs);

        var wrappedConverter = converterConfig.wrappedConfiguredConverterInstance(true);
        assertNotNull(wrappedConverter);

        Map<String,String> nestedConfigs = new HashMap<>();
        nestedConfigs.put("propA", "foo");
        nestedConfigs.put("propB", "bar");
        assertEquals(nestedConfigs, ((AssertionConverter)wrappedConverter).configs);
        assertTrue(((AssertionConverter)wrappedConverter).isKey);
    }

    @Test
    public void testInterceptorCount() {
        Map<String,String> configs = new HashMap<>();

        configs.put("wrapped.class", "com.tiki.lakehouse.kafka.connect.converter.AssertionConverter");
        configs.put("interceptors", "com.tiki.lakehouse.kafka.connect.converter.BarInterceptor,com.tiki.lakehouse.kafka.connect.converter.FooInterceptor");

        var converterConfig = new InterceptionConverterConfig(configs);

        var interceptors = converterConfig.interceptors();
        assertEquals(2, interceptors.size());

        assertEquals(interceptors.get(0).getClass().getCanonicalName(), "com.tiki.lakehouse.kafka.connect.converter.BarInterceptor");
        assertEquals(interceptors.get(1).getClass().getCanonicalName(), "com.tiki.lakehouse.kafka.connect.converter.FooInterceptor");
    }

    @Test
    public void testInterceptorMaterialization() {
        Map<String,String> configs = new HashMap<>();

        configs.put("wrapped.class", "com.tiki.lakehouse.kafka.connect.converter.AssertionConverter");
        configs.put("interceptors", "com.tiki.lakehouse.kafka.connect.converter.BarInterceptor");

        var converterConfig = new InterceptionConverterConfig(configs);

        var interceptors = converterConfig.interceptors();
        assertEquals(1, interceptors.size());

        Schema schema = new ConnectSchema(Schema.Type.INT8);

        assertNull(schema.parameters());

        var alteredSchema = interceptors.get(0).Apply(schema);

        assertTrue(alteredSchema.parameters().containsKey("bar-interceptor"));
        assertEquals("bar", alteredSchema.parameters().get("bar-interceptor"));
    }
}

