package de.bschwering.edi_workshop;

import org.apache.kafka.clients.admin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Configuration
public class Configurator {

    public static final String APP_ID = "01-newspaper";
    private static final int PARTITIONS = 10;

    public static final String EDITIONS = "editions";
    public static final String SUBSCRIPTIONS = "subscriptions";

    private static final Logger logger = LoggerFactory.getLogger(Configurator.class);

    Configurator() {
        setupTopics();
    }

    void setupTopics() {
        List<String> topics = Arrays.asList(EDITIONS, SUBSCRIPTIONS);

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        try (Admin admin = Admin.create(properties)) {
            ListTopicsResult result = admin.listTopics();
            Set<String> set = result.names().get();

            // Remove APP-ID topics, auto generated stuff
            for (String topic : set) {
                if (topic.startsWith(APP_ID + "-")) {
                    logger.info("Found " + topic + " ... remove!");
                    DeleteTopicsResult res = admin.deleteTopics(Arrays.asList(topic));
                    res.all().get();
                }
            }

            // Recreate topics
            for (String topic : topics) {
                if (set.contains(topic)) {
                    logger.info("Found " + topic + " ... remove!");
                    DeleteTopicsResult res = admin.deleteTopics(Arrays.asList(topic));
                    res.all().get();
                }
            }

            Thread.sleep(1000); // wait a little

            for (String topic : topics) {
                logger.info("Recreate " + topic + " with " + PARTITIONS + " partitions");
                NewTopic t = new NewTopic(topic, PARTITIONS, (short) 1);
                CreateTopicsResult res = admin.createTopics(Arrays.asList(t));
                res.values().get(topic);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

}
