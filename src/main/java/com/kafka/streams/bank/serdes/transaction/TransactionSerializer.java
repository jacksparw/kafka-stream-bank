package com.kafka.streams.bank.serdes.transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class TransactionSerializer implements Serializer<Transaction> {

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String data, Transaction transaction) {
        byte[] retVal = null;
        try {
            retVal = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }
}
