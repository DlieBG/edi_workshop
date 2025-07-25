package de.bschwering.edi_workshop;

import de.bschwering.edi_workshop.models.Edition;
import de.bschwering.edi_workshop.models.Subscription;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class Pipeline {

    @PostConstruct
    void buildPipeline() {
        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, Configurator.APP_ID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Void().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // -----------------------------------------------------------------------------------------------

        StreamsBuilder builder = new StreamsBuilder();

        // Sobald eine neue Edition veröffentlicht wird, soll für jede Subscription, die den Key der neuen Edition
        // abonniert, eine Nachricht auf die Konsole geschrieben werden.

        var build = builder.build();

        // -----------------------------------------------------------------------------------------------
        KafkaStreams streams = new KafkaStreams(build, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
