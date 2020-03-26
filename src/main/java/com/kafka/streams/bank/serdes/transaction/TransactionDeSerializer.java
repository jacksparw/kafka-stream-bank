package com.kafka.streams.bank.serdes.transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class TransactionDeSerializer implements Deserializer<Transaction> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Transaction deserialize(String s, byte[] bytes) {
        Transaction transaction = null;
        try {
            transaction = mapper.readValue(bytes, Transaction.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return transaction;
    }
}
