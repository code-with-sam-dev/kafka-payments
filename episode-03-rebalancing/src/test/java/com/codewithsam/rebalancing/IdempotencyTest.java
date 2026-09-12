package com.codewithsam.rebalancing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The claim this whole episode rests on.
 *
 * A rebalance reprocesses anything handled but not yet committed. You cannot
 * prevent the redelivery, so the defence has to be that the WORK refuses to
 * happen twice. These tests hold the ledger to that, including under the
 * concurrency a rebalance actually produces, where two members can briefly be
 * handling the same reassigned partition.
 *
 * No broker is needed to prove this part, and deliberately so: the property is
 * about the side effect, not about Kafka, and a test that needs a container to
 * assert it would be testing the container.
 */
class IdempotencyTest {

    @Test
    @DisplayName("the effect runs once, however many times the record is delivered")
    void effectRunsOnce() {
        PaymentLedger ledger = new PaymentLedger();

        assertThat(ledger.applyOnce("payment-1")).isTrue();
        assertThat(ledger.applyOnce("payment-1")).isFalse();
        assertThat(ledger.applyOnce("payment-1")).isFalse();

        assertThat(ledger.size()).isEqualTo(1);
    }

    @Test
    @DisplayName("different payments are independent")
    void differentPaymentsAreIndependent() {
        PaymentLedger ledger = new PaymentLedger();

        assertThat(ledger.applyOnce("payment-1")).isTrue();
        assertThat(ledger.applyOnce("payment-2")).isTrue();

        assertThat(ledger.size()).isEqualTo(2);
    }

    /**
     * The case a check-then-act would fail.
     *
     * During a rebalance the old owner may still be finishing a batch while the
     * new owner starts the same one. If the claim were "if not present, then
     * put", both would see it absent and both would perform the effect. The
     * claim has to be atomic, and this is what proves it is.
     */
    @Test
    @DisplayName("concurrent redelivery of the same payment still performs one effect")
    void concurrentRedeliveryPerformsOneEffect() throws Exception {
        PaymentLedger ledger = new PaymentLedger();
        int threads = 32;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startTogether = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(threads);
        AtomicInteger effectsPerformed = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    startTogether.await();
                    if (ledger.applyOnce("payment-contended")) {
                        effectsPerformed.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            });
        }

        startTogether.countDown();
        assertThat(finished.await(10, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        assertThat(effectsPerformed.get())
            .as("exactly one of %d concurrent deliveries may perform the effect", threads)
            .isEqualTo(1);
    }

    @Test
    @DisplayName("the log separates deliveries from effects, because the gap is the lesson")
    void logSeparatesDeliveriesFromEffects() {
        ProcessingLog log = new ProcessingLog();
        PaymentLedger ledger = new PaymentLedger();

        // Three deliveries of the same payment, which is what a rebalance
        // between processing and committing produces.
        for (int i = 0; i < 3; i++) {
            log.recordDelivery("payment-1");
            if (ledger.applyOnce("payment-1")) {
                log.recordEffect("payment-1");
            }
        }

        assertThat(log.deliveriesFor("payment-1")).isEqualTo(3);
        assertThat(log.effectsFor("payment-1")).isEqualTo(1);
        assertThat(log.redelivered()).containsEntry("payment-1", 3);
        assertThat(log.duplicatedEffects())
            .as("an idempotent handler must never duplicate an effect")
            .isEmpty();
    }
}
