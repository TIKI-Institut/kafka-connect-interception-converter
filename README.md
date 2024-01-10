
# Interception Kafka Connect Converter

This repo allows a user to reference an existing Kafka Connect converter and inject one or more interceptors to alter the 
connect schema before the conversion is performed.

Note: This example uses the confluent AvroConverter, which needs to exist in the classpath on its own. This project doesn't provide this dependency.

i.e. 
```
value.converter: io.confluent.connect.avro.AvroConverter
value.converter.enhanced.avro.schema.support: false
value.converter.schema.registry.url: http://schema-registry.strimzi.svc.cluster.local:8081
value.converter.schemas.enable: "true"
```

could be intercepted by using this configuration

```
value.converter: com.tiki.lakehouse.kafka.connect.converter.InterceptionConverter
value.converter.wrapped.class: io.confluent.connect.avro.AvroConverter
value.converter.wrapped.config.enhanced.avro.schema.support: false
value.converter.wrapped.config.schema.registry.url: http://schema-registry.strimzi.svc.cluster.local:8081
value.converter.wrapped.config.schemas.enable: "true"
value.converter.interceptors=com.tiki.lakehouse.kafka.connect.converter.interceptors.InitConnectDecimalFix
```

## Interceptors

- ### com.tiki.lakehouse.kafka.connect.converter.interceptors.InitConnectDecimalFix

  Fixes a missing property of the connect schema regarding Decimals (``org.apache.kafka.connect.data.Decimal``). 

  While using ``org.init.ohja.kafka.connect.odp.source.ODPSourceConnector`` you can observe the connect configuration below for Decimals.
  This configuration is missing the property ``connect.decimal.precision`` for kafka connect (using confluent) to successfully convert Decimals in a Avro Message using ``io.confluent.connect.avro.AvroConverter``.

  So:
  ```
  {
    "scale": "3",
    "jcoType": "JCoBCDType",
    "length": "7",
    "decimals": "3"
  }
  ```
  will be converted into : 
  ```
  {
    "scale": "3",
    "jcoType": "JCoBCDType",
    "length": "7",
    "decimals": "3",
    "connect.decimal.precision": "7"
  }
  ```    
  by cloning the property ``length``
