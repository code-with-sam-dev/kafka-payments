package com.codewithsam.rebalancing;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * The side effect, and the thing that makes it safe to repeat.
 *
 * "Idempotent" is easy to say and easy to get subtly wrong, so this is the
 * smallest honest version: before performing the effect, claim the payment id.
 * The claim either succeeds, in which case this delivery is the first and the
 * effect runs, or it fails, in which case some earlier delivery already did
 * the work and this one does nothing.
 *
 * `putIfAbsent` on a concurrent set is atomic, which matters: two members of
 * the group can be handling the same reassigned partition for a moment during
 * a rebalance, and a check-then-act would let both through.
 *
 * In a real system this claim is a unique constraint in the database, not a
 * set in memory. The shape is identical and so is the reasoning: the defence
 * against reprocessing is that the WORK refuses to happen twice, never that
 * the delivery promises not to arrive twice.
 */
@Component
public class PaymentLedger {

    private final Set<String> applied = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * @return true when this call performed the effect, false when an earlier
     *         delivery already had.
     */
    public boolean applyOnce(String paymentId) {
        return applied.add(paymentId);
    }

    public boolean hasApplied(String paymentId) {
        return applied.contains(paymentId);
    }

    public int size() {
        return applied.size();
    }

    public void clear() {
        applied.clear();
    }
}
