package com.kafka.streams.bank.serdes.balance;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class BalanceDeSerializer implements Deserializer<Balance> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Balance deserialize(String s, byte[] bytes) {
        Balance balance = null;
        try {
            balance = mapper.readValue(bytes, Balance.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return balance;
    }
}
