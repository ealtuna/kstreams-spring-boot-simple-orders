package com.ealtuna.springbootkstreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import lombok.Data;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Instant;

@Configuration
@EnableKafkaStreams
public class KStreamConfiguration {

    private static final String INPUT_TOPIC_NAME = "orders";
    public static final String OUTPUT_TOPIC_NAME = "even-orders";

    @Bean
    public KStream<String, Order> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, Order> input = kStreamBuilder.stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.String(), new JsonSerde<>(Order.class)));
        input.print(Printed.toSysOut());

        // Simple filter to stream all event orderid
        KStream<String, Order> output = input.filter((key, value) -> value.orderid % 2 == 0 );
        output.to(OUTPUT_TOPIC_NAME);
        return output;
    }

    @Data
    public static class Order {
        Long orderid;
        String itemid;
        Long ordertime;
        Double orderunits;
        Address address;

        @Data
        public static class Address {
            String city;
            String state;
            String zipcode;
        }
    }
}
