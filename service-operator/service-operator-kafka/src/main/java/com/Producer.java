package com;

import java.io.*;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
   static String topic = "logTopic";//定义主题
   static String file = "D:\\admin\\Desktop\\log";

    public static void main(String[] args) throws InterruptedException {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.10:9092");//kafka地址，多个地址用逗号分割
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);

        try {
            final BufferedReader bufferedReader;
            String line = null;
            bufferedReader = new BufferedReader(new FileReader(new File(file)));
            while (((line = bufferedReader.readLine()) != null)) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, line);
                Thread.sleep(500);
                kafkaProducer.send(record);
            }
            bufferedReader.close();
            kafkaProducer.close();
            System.out.println("消息发送完毕");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
