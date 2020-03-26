package com.kafka.streams.bank.serdes.transaction;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
public class Transaction {
    private String name;
    private int count;
    private Long amount;
    private Date time;
}
