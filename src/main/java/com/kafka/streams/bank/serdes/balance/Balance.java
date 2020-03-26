package com.kafka.streams.bank.serdes.balance;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
public class Balance {
    private int transactionCount;
    private Long balance = 0L;
    private Date lastTransactionTime;
}
