package de.bschwering.edi_workshop;

import de.bschwering.edi_workshop.models.Edition;
import de.bschwering.edi_workshop.models.Subscription;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

@Component
public class Generator {

    private static Producer<String, Object> producer;

    @Scheduled(fixedRate = 2000)
    void publishEditions() throws InterruptedException, UnknownHostException, ExecutionException {
        List<Edition> editions = List.of(
                new Edition("dk", "Donaukurier 2025-06-27", "Quatsch..."),
                new Edition("dk", "Donaukurier 2025-06-28", "Keine Ahnung..."),
                new Edition("bz", "Borkener Zeitung 2025-06-27", "Borkener Fakenews..."),
                new Edition("bz", "Horber Zeitung 2025-06-28", "800 Jahre!")
        );

        editions.forEach(createRecords(Configurator.EDITIONS, Edition::getKey));
    }

    @PostConstruct
    void publishSubscriptions() throws UnknownHostException, ExecutionException, InterruptedException {
        List<Subscription> subscriptions = List.of(
                new Subscription("a", "Birgit", "bz"),
                new Subscription("b", "Christoph", "bz"),
                new Subscription("c", "Benedikt", "bz"),
                new Subscription("d", "Benedikt", "dk"),
                new Subscription("e", "Jannik Nu", "dk")
        );

        subscriptions.forEach(createRecords(Configurator.SUBSCRIPTIONS, Subscription::getId));
    }

    private static <T> Consumer<T> createRecords(String topic)
            throws UnknownHostException, InterruptedException, ExecutionException {
        return createRecords(topic, (p) -> null);
    }

    private static <T> Consumer<T> createRecords(String topic, Function<T, String> key)
            throws UnknownHostException, InterruptedException, ExecutionException {
        Properties config = new Properties();
        Producer<String, Object> producer = getOrCreate(config);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
        return (p) -> {
            String apply = key.apply(p);
            ProducerRecord<String, Object> record = apply != null ? new ProducerRecord<>(topic, apply, p) : new ProducerRecord<>(topic, p);
            Future<RecordMetadata> future = producer.send(record);
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        };
    }

    private static Producer<String, Object> getOrCreate(Properties config) throws UnknownHostException {
        if(producer != null)
            return producer;
        config.put("client.id", InetAddress.getLocalHost().getHostName());
        config.put("bootstrap.servers", "localhost:9092");
        config.put("acks", "all");
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", JsonSerializer.class.getName());
        producer = new KafkaProducer<>(config);
        return producer;
    }

}
