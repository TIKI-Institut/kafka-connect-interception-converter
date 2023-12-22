package com.tiki.lakehouse.kafka.connect.converter;
import org.apache.kafka.connect.data.Schema;

public interface Interceptor {
    Schema Apply(Schema target ) ;
}
