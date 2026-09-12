package com.codewithsam.rebalancing;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * The consumer that gets thrown out of its own group for being slow.
 *
 * Nothing here is broken, and that is the point. The process is healthy, the
 * heartbeat thread is running on schedule the entire time, and the code has no
 * bug in it. It simply takes longer inside one poll than
 * `max.poll.interval.ms` allows, which the coordinator treats as failure.
 *
 * The deliberate slowness is configuration, not a hard coded sleep, so the
 * demo can be run in the fast mode used by tests and the slow mode used to
 * actually watch an eviction happen.
 *
 * EFFECT VS DELIVERY. This handler records the delivery and then performs the
 * effect through the ledger, which refuses to run twice for the same payment.
 * After an eviction the redelivered records show up as deliveries above one
 * while the effect count stays at one, and the gap between those two numbers
 * is the thing the episode is about.
 */
@Component
public class SlowConsumer {

    private final ProcessingLog log;
    private final PaymentLedger ledger;

    /**
     * Milliseconds spent on each record.
     *
     * Set above `max.poll.interval.ms` divided by `max.poll.records` and the
     * group will evict this member mid batch. That is the whole trick.
     */
    @Value("${demo.processing-millis:0}")
    private long processingMillis;

    public SlowConsumer(ProcessingLog log, PaymentLedger ledger) {
        this.log = log;
        this.ledger = ledger;
    }

    @KafkaListener(
        topics = PaymentProducer.TOPIC,
        groupId = "${demo.group-id:payments-demo}",
        containerFactory = "batchFactory",
        batch = "true")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        for (ConsumerRecord<String, String> record : records) {
            PaymentEvent event = PaymentEvent.parse(record.value());

            // Every arrival is counted, whether or not it does any work.
            log.recordDelivery(event.paymentId());

            burnTime();

            // The claim is atomic, so a redelivery after a rebalance, or two
            // members briefly overlapping on the same partition, still yields
            // exactly one effect.
            if (ledger.applyOnce(event.paymentId())) {
                log.recordEffect(event.paymentId());
            }
        }
    }

    private void burnTime() {
        if (processingMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(processingMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
