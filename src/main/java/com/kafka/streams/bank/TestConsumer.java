package com.kafka.streams.bank;

import com.kafka.streams.bank.serdes.transaction.Transaction;
import com.kafka.streams.bank.serdes.transaction.TransactionDeSerializer;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Log4j
public class TestConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeSerializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-bank-consume");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Transaction> kafkaConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new TransactionDeSerializer());
        kafkaConsumer.subscribe(Arrays.asList("bank-transactions"));

        while (Boolean.TRUE) {
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, Transaction> consumerRecord : consumerRecords) {
                log.info("Partition: " + consumerRecord.partition() +
                        ", Offset: " + consumerRecord.offset() +
                        ", Key: " + consumerRecord.key() +
                        ", Value: " + consumerRecord.value());
                Headers headers = consumerRecord.headers();
                headers.forEach(e -> {
                    log.info(e.key());
                    log.info(new String(e.value()));
                });

            }
            kafkaConsumer.commitSync();
        }

    }
}
