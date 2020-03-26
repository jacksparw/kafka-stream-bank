package com.kafka.streams.bank.serdes.balance;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class BalanceSerializer implements Serializer<Balance> {

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String data, Balance balance) {
        byte[] retVal = null;
        try {
            retVal = objectMapper.writeValueAsString(balance).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }
}
