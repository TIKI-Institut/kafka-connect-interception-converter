package com.tiki.lakehouse.kafka.connect.converter;

import org.apache.kafka.connect.data.Schema;

public class FooInterceptor extends BaseInterceptor {
    @Override
    public Schema Apply(Schema target) {
        return cloneSchema(target, s -> s.parameter("foo-interceptor", "foo"));

    }
}
