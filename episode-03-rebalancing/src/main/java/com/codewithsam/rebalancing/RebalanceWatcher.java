package com.codewithsam.rebalancing;

import java.util.Collection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

/**
 * Counts rebalances, and says which kind they were.
 *
 * Without this the demo would show duplicates with no visible cause, which is
 * exactly the experience the episode is trying to explain rather than
 * reproduce. Printing the revocation makes the sequence legible: partitions
 * revoked, partitions assigned, the same records arriving again.
 *
 * The distinction that matters is BEFORE COMMIT versus AFTER COMMIT.
 * `onPartitionsRevokedBeforeCommit` fires while there is still uncommitted
 * work in flight, which is precisely the window that produces the duplicate.
 * Counting those separately is what lets the demo claim the duplicate was
 * caused by the eviction rather than by something else.
 */
@Component
public class RebalanceWatcher implements ConsumerAwareRebalanceListener {

    private final ProcessingLog log;

    public RebalanceWatcher(ProcessingLog log) {
        this.log = log;
    }

    @Override
    public void onPartitionsRevokedBeforeCommit(
        Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        // Work was in flight and the offsets for it are not committed yet.
        // Whatever was processed in this window will arrive again.
        log.recordEviction();
        System.out.printf(
            "REVOKED BEFORE COMMIT  %s  <- anything processed and not committed will repeat%n",
            partitions);
    }

    @Override
    public void onPartitionsAssigned(
        Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        log.recordRebalance();
        System.out.printf("ASSIGNED  %s%n", partitions);
    }
}
