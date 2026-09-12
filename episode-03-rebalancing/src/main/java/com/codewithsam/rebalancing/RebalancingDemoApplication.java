package com.codewithsam.rebalancing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka rebalancing, demonstrated rather than asserted.
 *
 * The episode's claim is that a consumer can be perfectly alive and still be
 * thrown out of its group, and that the duplicate which follows was always
 * going to follow. This module makes that happen on purpose, in one command,
 * so it can be watched rather than believed.
 *
 * How it is forced. `max.poll.interval.ms` is set to a few seconds and the
 * handler deliberately spends longer than that on a batch. The heartbeat
 * thread keeps reporting the process alive the entire time. The coordinator
 * evicts the member anyway, for being SLOW rather than for being down, and
 * reassigns its partitions. Whatever had been processed but not yet committed
 * is then processed again by the new owner.
 *
 * Two handlers consume the same records:
 *
 *   AT LEAST ONCE   processes and counts every delivery, so the duplicate is
 *                   visible as a delivery count above one
 *   IDEMPOTENT      does the same work keyed on the payment id, so repeated
 *                   delivery produces exactly one effect
 *
 * Same broker, same records, same eviction. Different outcome. That difference
 * is the lesson, and it is the reason the sheet says idempotency is the only
 * real defence.
 */
@SpringBootApplication
public class RebalancingDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(RebalancingDemoApplication.class, args);
    }
}
