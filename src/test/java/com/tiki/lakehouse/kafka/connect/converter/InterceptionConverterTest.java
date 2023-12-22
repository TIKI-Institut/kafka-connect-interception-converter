package com.tiki.lakehouse.kafka.connect.converter;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class InterceptionConverterTest {

    @Test
    public void testAlteredSchema() {

        var converter = new InterceptionConverter();

        assertNull(converter.wrappedConverter);
        assertNull(converter.interceptors);

        Map<String,String> configs = new HashMap<>();
        configs.put("wrapped.class", "com.tiki.lakehouse.kafka.connect.converter.AssertionConverter");
        configs.put("wrapped.config.propA", "foo");
        configs.put("wrapped.config.propB", "bar");
        configs.put("interceptors", "com.tiki.lakehouse.kafka.connect.converter.BarInterceptor,com.tiki.lakehouse.kafka.connect.converter.FooInterceptor");
        converter.configure(configs,false);

        assertNotNull(converter.wrappedConverter);
        assertNotNull(converter.interceptors);

        var data =  converter.fromConnectData("foo", Schema.INT32_SCHEMA, Integer.valueOf(4));
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, data);

        var conversionSchema = ((AssertionConverter)converter.wrappedConverter).conversionSchema;

        assertEquals(Schema.INT32_SCHEMA.name(), conversionSchema.name());
        assertEquals(Schema.INT32_SCHEMA.type(), conversionSchema.type());
        assertEquals(Schema.INT32_SCHEMA.isOptional(), conversionSchema.isOptional());
        assertEquals(Schema.INT32_SCHEMA.version(), conversionSchema.version());
        Map<String,String> expectedParameters = new HashMap<>();
        expectedParameters.put("bar-interceptor", "bar");
        expectedParameters.put("foo-interceptor", "foo");
        assertEquals(expectedParameters, conversionSchema.parameters());
    }


}
