package com.demo.upimesh.model;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.math.BigDecimal;
import java.util.List;

public interface TransactionRepository extends JpaRepository<Transaction, Long> {
    List<Transaction> findTop20ByOrderByIdDesc();
    boolean existsByPacketHash(String packetHash);

    @Query("SELECT COUNT(t) FROM Transaction t WHERE t.status = 'SETTLED'")
    long countSettled();

    @Query("SELECT COALESCE(SUM(t.amount), 0) FROM Transaction t WHERE t.status = 'SETTLED'")
    BigDecimal sumSettledAmount();

    @Query("SELECT COALESCE(AVG(t.hopCount), 0) FROM Transaction t WHERE t.status = 'SETTLED'")
    Double avgHopCount();
}
