package com.codewithsam.ordering;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.stereotype.Component;

/**
 * Records the order in which events were actually PROCESSED, per consumer
 * strategy, so the demo can prove what happened instead of describing it.
 *
 * Kafka's delivery order is not in question here - it is always correct. What
 * this captures is what the application did with that correct delivery.
 */
@Component
public class ProcessingLog {

    private final Map<String, List<PaymentEvent>> byStrategy = new ConcurrentHashMap<>();

    public void record(String strategy, PaymentEvent event) {
        byStrategy
            .computeIfAbsent(strategy, k -> new CopyOnWriteArrayList<>())
            .add(event);
    }

    public List<PaymentEvent> get(String strategy) {
        return byStrategy.getOrDefault(strategy, List.of());
    }

    public void clear() {
        byStrategy.clear();
    }

    /**
     * True when every payment's events were processed in ascending sequence.
     * This is the assertion the whole demo exists to make.
     */
    public boolean isOrdered(String strategy) {
        return isOrderedFor(strategy, null);
    }

    /**
     * As {@link #isOrdered}, but limited to the given payment ids.
     *
     * Tests need this because both consumers subscribe to the same topic and
     * drain at different speeds - so a test can still be seeing records from a
     * previous test's payments. Scoping the assertion to ids the caller owns is
     * real isolation; clearing shared state while async consumers are live is
     * merely a race.
     *
     * @param paymentIds ids to consider, or null for all
     */
    public boolean isOrderedFor(String strategy, Collection<String> paymentIds) {
        Map<String, Integer> lastSeen = new HashMap<>();
        for (PaymentEvent e : get(strategy)) {
            if (paymentIds != null && !paymentIds.contains(e.paymentId())) {
                continue;
            }
            int previous = lastSeen.getOrDefault(e.paymentId(), 0);
            if (e.sequence() < previous) {
                return false;
            }
            lastSeen.put(e.paymentId(), e.sequence());
        }
        return true;
    }

    /** How many records for the given payments this strategy has processed. */
    public long countFor(String strategy, Collection<String> paymentIds) {
        return get(strategy).stream()
            .filter(e -> paymentIds == null || paymentIds.contains(e.paymentId()))
            .count();
    }
}
