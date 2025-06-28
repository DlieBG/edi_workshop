package de.bschwering.edi_workshop;

import de.bschwering.edi_workshop.models.*;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
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

    @Scheduled(initialDelay = 5000, fixedRate = 2000)
    void publishTransactions() throws InterruptedException, UnknownHostException, ExecutionException {
        List<Transaction> transactions = List.of(
                new Transaction("gf-1", "Automat 1", -1.8),
                new Transaction("bs-1", "Essensausgabe", -2.5),
                new Transaction("bs-1", "Kaffe", -1.8),
                new Transaction("bs-1", "Aufwerter", 5)
        );

        transactions.forEach(createRecords(Configurator.TRANSACTIONS, Transaction::getWallet));
    }

    @Scheduled(initialDelay = 5000, fixedRate = Long.MAX_VALUE)
    void publishWallets() throws UnknownHostException, ExecutionException, InterruptedException {
        List<Wallet> wallets = List.of(
                new Wallet("bs-1", "Benedikt", 25.10),
                new Wallet("mb-1", "Marius", 1.10),
                new Wallet("re-1", "Robert", 42.11),
                new Wallet("gf-1", "Johannes", 6.56)
        );

        wallets.forEach(createRecords(Configurator.WALLETS, Wallet::getId));
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
