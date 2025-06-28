package de.bschwering.edi_workshop;

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
    void buildPipeline() throws InterruptedException {
        Thread.sleep(5000);
        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, Configurator.APP_ID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Void().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // -----------------------------------------------------------------------------------------------

        StreamsBuilder builder = new StreamsBuilder();

        // Für jede neue Transaktion soll der Guthabenstand des Wallets geupdated werden.
        // Es soll eine Fehlermeldung ausgegeben werden, wenn der Guthabenstand negativ wird.

        var transactions = builder.stream(Configurator.TRANSACTIONS, Consumed.with(Serdes.String(), JsonSerdes.transactionSerde()));
        var wallets = builder.table(Configurator.WALLETS, Consumed.with(Serdes.String(), JsonSerdes.walletSerde()));

        transactions.join(
                wallets,
                (transaction, wallet) -> {
                    wallet.setBalance(
                            wallet.getBalance()
                            + transaction.getAmount()
                    );

                    if (wallet.getBalance() < 0)
                        System.out.println("Student (" + wallet.getName() + "), du hast kein Geld: " + wallet.getBalance());

                    System.out.println("Student (" + wallet.getName() + "), neue Transaktion: " + transaction.getAmount() + " und Kontostand: " + wallet.getBalance());

                    return wallet;
                }
        ).to(Configurator.WALLETS, Produced.with(Serdes.String(), JsonSerdes.walletSerde()));

        var build = builder.build();

        // -----------------------------------------------------------------------------------------------
        KafkaStreams streams = new KafkaStreams(build, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
