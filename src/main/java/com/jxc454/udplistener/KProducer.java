package com.jxc454.udplistener;

import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

class KProducer {
    final static Logger logger = LogManager.getLogger(App.class);

    KafkaProducer<String, Integer> producer = null;

    KProducer(Config config) {
        this.produce(config);
    }

    private void produce(Config config) {
        Properties props = new Properties();

        logger.info("PRODUCING");

        props.put("bootstrap.servers", config.getString("bootstrap.servers"));
        props.put("acks", config.getString("acks"));
        props.put("retries", config.getInt("retries"));
        props.put("batch.size", config.getInt("batch.size"));
        props.put("linger.ms", config.getInt("linger.ms"));
        props.put("buffer.memory", config.getInt("buffer.memory"));
        props.put("key.serializer", config.getString("key.serializer"));
        props.put("value.serializer", config.getString("value.serializer"));

        try {
            this.producer = new KafkaProducer<>(props);
        } catch (Exception e) {
            logger.error("Failed to start kafka producer", e);
            throw new RuntimeException(e);
        }
        logger.info("Kafka Producer started....");
    }
}
