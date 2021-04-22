package com;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * kafkaProducer多例工具类
 */
public class KafkaProducerOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerOperator.class);

    private KafkaProducer<String, String> producer;
    private String topic;

    public KafkaProducerOperator(String topic,Properties properties) {
        this.topic = topic;
        producer = new KafkaProducer<>(properties);
        LOGGER.info("kafka producer initialize success ");
    }


    /**
     * producer1 发送消息不考虑返回信息
     */
    public void sendMessageForgotResult(String msg) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        producer.send(record);
    }


    /**
     * producer2 发送消息同步等到发送成功
     */
    public void sendMessageSync(String msg) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.error("kafka发送消息失败:" + e.getMessage(),
                    e);
            retryKakfaMessage(msg);
        }
    }


    /**
     * producer3 发送消息异步回调返回消息
     */
    public void sendMessageCallBack(final String msg) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (null != e) {
                    LOGGER.error("kafka发送消息失败:" + e.getMessage(),
                            e);
                    retryKakfaMessage(msg);
                }
            }
        });
    }

    /**
     * 当kafka消息发送失败后,重试
     *
     * @param retryMessage
     */
    public void retryKakfaMessage(final String retryMessage) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                topic, retryMessage);
        int retry = 3;
        for (int i = 1; i <= retry; i++) {
            try {
                producer.send(record);
                return;
            } catch (Exception e) {
                LOGGER.error("kafka发送消息重试失败:" + e.getMessage(), e);
                retryKakfaMessage(retryMessage);
            }
        }
    }


    /**
     * kafka实例销毁
     */
    public void close() {
        if (null != producer) {
            producer.close();
        }
    }
}