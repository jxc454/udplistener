package com.github.jxc454.udplistener;

import org.apache.kafka.clients.producer.KafkaProducer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.*;
import java.util.UUID;

public class App {
    private final static Logger logger = LogManager.getLogger(App.class);

    public static void main( String[] args ) throws Exception
    {
        DatagramSocket serverSocket = new DatagramSocket(5140);
        byte[] receiveData = new byte[64000];

        Config config = ConfigFactory.parseResources("producer.conf");

        KafkaProducer<String, Integer> p = new KProducer(config).producer;

        while (true)
        {
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            serverSocket.receive(receivePacket);

            Integer number = tryParse((new String(receivePacket.getData())).trim());

            if (number == null) {
                logger.warn((new String(receivePacket.getData())).trim() + " is not a number, ignoring.");
            } else {
                logger.info("got number: " + number);
                p.send(new ProducerRecord<>(config.getString("topic"), UUID.randomUUID().toString(), number));
            }
            receiveData = new byte[64000];
        }
    }

    private static Integer tryParse(String text) {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
