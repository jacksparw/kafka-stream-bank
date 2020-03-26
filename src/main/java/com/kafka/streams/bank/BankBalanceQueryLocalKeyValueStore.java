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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class BankBalanceQueryLocalKeyValueStore {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-stream-app");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        StreamsBuilder builder = new StreamsBuilder();

        Serde<Transaction> transactionSerde = Serdes.serdeFrom(new TransactionSerializer(), new TransactionDeSerializer());

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
                );

        KafkaStreams streams = new KafkaStreams(builder.build(), config);


        startKafkaStreams(streams);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void startKafkaStreams(KafkaStreams streams) {
        CompletableFuture<KafkaStreams.State> stateFuture = new CompletableFuture<>();
        streams.setStateListener((newState, oldState) -> kafkaStateListener(stateFuture, newState));

        streams.start();

        try {
            KafkaStreams.State finalState = stateFuture.get();
            if (finalState == KafkaStreams.State.RUNNING) {
                queryLocalKeyValueStore(streams);
            }
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        } catch (ExecutionException ex) {
            ex.printStackTrace();
        }
    }

    private static void kafkaStateListener(CompletableFuture<KafkaStreams.State> stateFuture, KafkaStreams.State newState) {
        if (stateFuture.isDone()) {
            return;
        }

        if (newState == KafkaStreams.State.RUNNING || newState == KafkaStreams.State.ERROR) {
            stateFuture.complete(newState);
        }
    }

    private static void queryLocalKeyValueStore(KafkaStreams streams) {

        ReadOnlyKeyValueStore<String, Balance> keyValueStore =
                streams.store("bank-balance-stream",
                        QueryableStoreTypes.keyValueStore());

        System.out.println("john:" + keyValueStore.get("john"));
    }

    private static Balance newBalance(Transaction transaction, Balance balance) {
        Balance newBalance = new Balance();
        newBalance.setBalance(balance.getBalance() + transaction.getAmount());
        newBalance.setTransactionCount(balance.getTransactionCount() + 1);
        newBalance.setLastTransactionTime(transaction.getTime());
        return newBalance;
    }
}
