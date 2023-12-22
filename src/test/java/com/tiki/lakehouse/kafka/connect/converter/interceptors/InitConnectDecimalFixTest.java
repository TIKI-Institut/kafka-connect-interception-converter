package com.tiki.lakehouse.kafka.connect.converter.interceptors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class InitConnectDecimalFixTest {

    private SchemaBuilder decimalSchema(Schema.Type type) {
        return SchemaBuilder
                .type(type)
                .name("org.apache.kafka.connect.data.Decimal")
                .version(1)
                .parameter("scale", "3")
                .parameter("decimals", "5");
    }

    @Test
    public void testAlteredSchema() {

        var interceptor = new InitConnectDecimalFix();
        var resultSchema = interceptor.Apply(decimalSchema(Schema.Type.BYTES));

        Map<String,String> expectedParameters = new HashMap<>();
        expectedParameters.put("scale", "3");
        expectedParameters.put("decimals", "5");
        expectedParameters.put("connect.decimal.precision", "5");

        assertEquals(expectedParameters, resultSchema.parameters());
    }


    @Test
    public void testIgnoreIncompatibleSchema() {

        var interceptor = new InitConnectDecimalFix();
        var resultSchema = interceptor.Apply(decimalSchema(Schema.Type.FLOAT64));

        Map<String,String> expectedParameters = new HashMap<>();
        expectedParameters.put("scale", "3");
        expectedParameters.put("decimals", "5");

        assertEquals(expectedParameters, resultSchema.parameters());
    }

    @Test
    public void testIgnoreAlreadyConfiguredSchema() {

        var interceptor = new InitConnectDecimalFix();
        var resultSchema = interceptor.Apply(decimalSchema(Schema.Type.FLOAT64).parameter("connect.decimal.precision", "foo-bar"));

        Map<String,String> expectedParameters = new HashMap<>();
        expectedParameters.put("scale", "3");
        expectedParameters.put("decimals", "5");
        expectedParameters.put("connect.decimal.precision", "foo-bar");

        assertEquals(expectedParameters, resultSchema.parameters());
    }

}
