package com.codewithsam.rebalancing;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * What actually happened, counted.
 *
 * Two numbers per payment, and the gap between them is the entire point:
 *
 *   deliveries   how many times a record for this payment reached a handler
 *   effects      how many times the business action was actually performed
 *
 * At least once delivery means DELIVERIES can exceed one after a rebalance.
 * Idempotent processing means EFFECTS must not. A demo that only reported
 * "it worked" would hide exactly the thing worth seeing, so both are kept.
 */
@Component
public class ProcessingLog {

    private final Map<String, AtomicInteger> deliveries = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> effects = new ConcurrentHashMap<>();
    private final AtomicInteger rebalances = new AtomicInteger();
    private final AtomicInteger evictions = new AtomicInteger();

    public void recordDelivery(String paymentId) {
        deliveries.computeIfAbsent(paymentId, k -> new AtomicInteger()).incrementAndGet();
    }

    public void recordEffect(String paymentId) {
        effects.computeIfAbsent(paymentId, k -> new AtomicInteger()).incrementAndGet();
    }

    public void recordRebalance() {
        rebalances.incrementAndGet();
    }

    /** A revocation that happened while the handler was still mid batch. */
    public void recordEviction() {
        evictions.incrementAndGet();
    }

    public int deliveriesFor(String paymentId) {
        AtomicInteger n = deliveries.get(paymentId);
        return n == null ? 0 : n.get();
    }

    public int effectsFor(String paymentId) {
        AtomicInteger n = effects.get(paymentId);
        return n == null ? 0 : n.get();
    }

    public int rebalanceCount() {
        return rebalances.get();
    }

    public int evictionCount() {
        return evictions.get();
    }

    /** Payments that reached a handler more than once. The duplicates. */
    public Map<String, Integer> redelivered() {
        Map<String, Integer> out = new LinkedHashMap<>();
        deliveries.forEach((id, n) -> {
            if (n.get() > 1) {
                out.put(id, n.get());
            }
        });
        return out;
    }

    /**
     * Payments whose EFFECT ran more than once.
     *
     * This is the map that should always be empty when the idempotent handler
     * is used. If it is not, the idempotency is broken, and the demo is then
     * telling the truth about that too.
     */
    public Map<String, Integer> duplicatedEffects() {
        Map<String, Integer> out = new LinkedHashMap<>();
        effects.forEach((id, n) -> {
            if (n.get() > 1) {
                out.put(id, n.get());
            }
        });
        return out;
    }

    public Map<String, Integer> allDeliveries() {
        Map<String, Integer> out = new LinkedHashMap<>();
        deliveries.forEach((id, n) -> out.put(id, n.get()));
        return out;
    }

    public void clear() {
        deliveries.clear();
        effects.clear();
        rebalances.set(0);
        evictions.set(0);
    }
}
