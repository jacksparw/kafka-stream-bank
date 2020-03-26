package com.kafka.streams.bank;

import com.kafka.streams.bank.serdes.balance.Balance;
import com.kafka.streams.bank.serdes.balance.BalanceDeSerializer;
import com.kafka.streams.bank.serdes.balance.BalanceSerializer;
import com.kafka.streams.bank.serdes.transaction.Transaction;
import com.kafka.streams.bank.serdes.transaction.TransactionDeSerializer;
import com.kafka.streams.bank.serdes.transaction.TransactionSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

public class BankBalanceExactlyOnceApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-stream-app");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        StreamsBuilder builder = new StreamsBuilder();

        TransactionDeSerializer transactionDeSerializer = new TransactionDeSerializer();
        TransactionSerializer transactionSerializer = new TransactionSerializer();
        Serde<Transaction> transactionSerde = Serdes.serdeFrom(transactionSerializer, transactionDeSerializer);

        BalanceSerializer balanceSerializer = new BalanceSerializer();
        BalanceDeSerializer balanceDeSerializer = new BalanceDeSerializer();
        Serde<Balance> balanceSerde = Serdes.serdeFrom(balanceSerializer, balanceDeSerializer);


        KStream<String, Transaction> transactionRecords = builder
                .stream("bank-transactions", Consumed.with(Serdes.String(), transactionSerde));

        transactionRecords
                .groupByKey()
                .aggregate(
                        Balance::new,
                        (user, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.<String, Balance, KeyValueStore<Bytes, byte[]>>as("bank-balance-stream")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(balanceSerde)
                )
                .toStream()
                .to("bank-balance-exactly-once", Produced.with(Serdes.String(), balanceSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static Balance newBalance(Transaction transaction, Balance balance) {
        Balance newBalance = new Balance();
        newBalance.setBalance(balance.getBalance() + transaction.getAmount());
        newBalance.setTransactionCount(balance.getTransactionCount() + 1);
        newBalance.setLastTransactionTime(transaction.getTime());
        return newBalance;
    }
}
