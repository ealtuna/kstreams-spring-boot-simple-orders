// NOT WORKING
package com.ealtuna.springbootkstreams;

import javafx.util.Pair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import lombok.Data;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Configuration
@EnableKafkaStreams
public class KStreamConfiguration {

    static final Long UNIQUE_TIME = Instant.now().getEpochSecond() + 10000;

    @Data
    public static class Event {
        Integer employeeId;
        Long startDate;
    }

    @Data
    public static class Snapshot {
        Integer employeeId;
        Long startDate;
        EffectiveName[] name;

        @Data
        public static class EffectiveName {
            String value;
            Long effectiveDate;
        }
    }

    @Data
    public static class EffectiveSnapshot {
        Integer employeeId;
        Long startDate;
        String name;
    }

    @Bean
    public KStream<String, Snapshot> eventToSnapshot(StreamsBuilder kStreamBuilder) {
        KStream<String, Event> events = kStreamBuilder.stream("events", Consumed.with(Serdes.String(), new JsonSerde<>(Event.class)));
        KStream<String, Snapshot> snapshots = events.mapValues((event) -> {
            Snapshot snapshot = new Snapshot();
            snapshot.employeeId = event.employeeId;
            snapshot.startDate = event.startDate;

            List<Snapshot.EffectiveName> effectiveNames = new ArrayList();
            Snapshot.EffectiveName effectiveName = new Snapshot.EffectiveName();
            effectiveName.value = "Enrique";
            effectiveName.effectiveDate = UNIQUE_TIME;
            effectiveNames.add(effectiveName);
            Snapshot.EffectiveName[] itemsArray = new Snapshot.EffectiveName[effectiveNames.size()];
            snapshot.name = effectiveNames.toArray(itemsArray);
            return snapshot;
        });
        snapshots.to("snapshots", Produced.with(Serdes.String(), new JsonSerde<>(Snapshot.class)));
        return snapshots;
    }

    class FilterDuplicatesTransformer implements ValueTransformerWithKey<String, EffectiveSnapshot, EffectiveSnapshot> {
        KeyValueStore<String, EffectiveSnapshot> kvStore;

        @Override
        public void init(ProcessorContext context) {
            kvStore = (KeyValueStore<String, EffectiveSnapshot>) context.getStateStore("output-effective-snapshots");
        }

        @Override
        public EffectiveSnapshot transform(String key, EffectiveSnapshot newValue) {
            EffectiveSnapshot oldValue = kvStore.get(key);
            if (oldValue == null || !oldValue.equals(newValue)) return newValue;
            return null;
        }

        @Override
        public void close() {}
    }

    @Bean
    public KStream<String, EffectiveSnapshot> toEffectiveSnapshot(StreamsBuilder kStreamBuilder) {
        KStream<String, Snapshot> snapshots = kStreamBuilder.stream("snapshots", Consumed.with(Serdes.String(), new JsonSerde<>(Snapshot.class)));
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore("output-effective-snapshots");
        KTable<String, EffectiveSnapshot> effectiveSnapshotsTable = kStreamBuilder.table(
                "effective-snapshots",
                Materialized.<String, EffectiveSnapshot>as(storeSupplier)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(new JsonSerde(EffectiveSnapshot.class))
                    .withCachingDisabled()
        );

        // calculate effective snapshot
        KStream<String, EffectiveSnapshot> effectiveSnapshots = snapshots.mapValues(s -> {
            EffectiveSnapshot effectiveSnapshot = new EffectiveSnapshot();
            effectiveSnapshot.employeeId = s.employeeId;
            effectiveSnapshot.name = Arrays.stream(s.name).map(e -> e.value).findFirst().orElse(null);
            effectiveSnapshot.startDate = s.startDate;
            return effectiveSnapshot;
        });

        // Remove duplicates
        /*effectiveSnapshots.leftJoin(effectiveSnapshotsTable, Pair::new)
            .flatMapValues(p -> {
                System.out.println("Comparing " + p);
                return p.getKey().equals(p.getValue()) ? Collections.emptyList() : Collections.singletonList(p.getKey());
            })
            .peek((k,v) -> System.out.println(k))*/
        effectiveSnapshots.transformValues(FilterDuplicatesTransformer::new, "output-effective-snapshots")
            .filter((k,v) -> v != null)
            .peek((k,v) -> System.out.println(k))
            .to("effective-snapshots", Produced.with(Serdes.String(), new JsonSerde<>(EffectiveSnapshot.class)));

        return effectiveSnapshots;
    }

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

    /*@Bean
    public KStream<String, Order> filterEvenOrders(StreamsBuilder kStreamBuilder) {
        KStream<String, Order> input = kStreamBuilder.stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.String(), new JsonSerde<>(Order.class)));
        // input.print(Printed.toSysOut());

        new KafkaStreamBrancher<String, Order>()
                .branch((k, v) -> v.orderid % 2 == 0, ks -> ks.print(Printed.toSysOut()))
                .onTopOf(input)
                .print(Printed.toSysOut());

        // Simple filter to stream all event orderid
        KStream<String, Order> output = input.filter((key, value) -> value.orderid % 2 == 0 );
        output.to(OUTPUT_TOPIC_NAME);
        return output;
    }*/
}
