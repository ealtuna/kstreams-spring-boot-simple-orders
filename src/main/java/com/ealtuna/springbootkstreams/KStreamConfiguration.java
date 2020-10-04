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

    /*
    if want to create more fine grain configuration you can use a Bean like:

    @Autowired private KafkaProperties kafkaProperties;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-streams");
        return new StreamsConfig(props);
    }
    * */

    @Bean
    public KStream<String, Order> filterEvenOrders(StreamsBuilder kStreamBuilder) {
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
