package com.cotter;

import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

import java.util.Properties;

class KProducer {

    private static Logger log = Logger.getLogger(KProducer.class.getName());
    KafkaProducer<Integer, String> producer = null;

    KProducer(Config config) {
        this.produce(config);
    }

    private void produce(Config config) {
        Properties props = new Properties();

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
            log.error("Failed to start kafka producer", e);
            throw new RuntimeException(e);
        }
        log.info("Kafka Producer is started....");
    }
}
