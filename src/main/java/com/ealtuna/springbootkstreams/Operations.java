package com.ealtuna.springbootkstreams;

import com.ealtuna.springbootkstreams.KStreamConfiguration.Order;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Reducer;

public class Operations {
    /**
     * .map(new OrderKeyValueMapper())
     * .groupByKey()
     * .reduce(new OrderReducer(), Materialized.<String, Order, KeyValueStore<Bytes, byte[]>>as("streams-json-store"))
     *
     * combinedDocuments.toStream().to("streams-json-output", Produced.with(Serdes.String(), new JsonSerde<>(Test.class)));
     */

    public static class OrderKeyValueMapper implements KeyValueMapper<String, Order, KeyValue<String, Order>> {

        @Override
        public KeyValue<String, Order> apply(String key, Order value) {
            return new KeyValue<String, Order>(value.orderid.toString(), value);
        }

    }

    public static class OrderReducer implements Reducer<Order> {

        @Override
        public Order apply(Order value1, Order value2) {
            // perform some reduce operation
            // value1.getWords().addAll(value2.getWords());
            return value1;
        }

    }
}
