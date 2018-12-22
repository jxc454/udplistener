package com.cotter;

import org.apache.kafka.clients.producer.KafkaProducer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.net.*;

public class App 
{
    public static void main( String[] args ) throws Exception
    {
        DatagramSocket serverSocket = new DatagramSocket(5140);
        byte[] receiveData = new byte[64000];
        Integer counter = 0;

        Config config = ConfigFactory.parseResources("producer.conf");

        KafkaProducer<Integer, String> p = new KProducer(config).producer;

        while (true)
        {
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            serverSocket.receive(receivePacket);

            String sentence = new String( receivePacket.getData());

            System.out.println(sentence);

            p.send(new ProducerRecord<>(config.getString("topic"), ++counter, sentence));
            receiveData = new byte[64000];
        }
    }
}
