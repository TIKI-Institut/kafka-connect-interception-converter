package com.tiki.lakehouse.kafka.connect.converter.interceptors;

import com.tiki.lakehouse.kafka.connect.converter.BaseInterceptor;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;

public class InitConnectDecimalFix extends BaseInterceptor {

    public static final String INIT_CONNECT_PRECISION_PROP = "decimals";
    static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";

    @Override
    public Schema Apply(Schema target) {
        return this.cloneSchema(target, s -> {

            var schemaParameters = s.parameters();

            //Connect Schemas for 'Decimals' are handled by using the logical type 'decimal' stored in a byte array
            if (s.type() == Schema.Type.BYTES &&
                    Decimal.LOGICAL_NAME.equalsIgnoreCase(s.name()) &&
                    schemaParameters != null &&
                    schemaParameters.containsKey(INIT_CONNECT_PRECISION_PROP) &&
                    !schemaParameters.containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP)) {
                s.parameter(CONNECT_AVRO_DECIMAL_PRECISION_PROP, schemaParameters.get(INIT_CONNECT_PRECISION_PROP));
            }

            return s;
        });
    }


}
