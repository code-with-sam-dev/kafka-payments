package com.codewithsam.rebalancing;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Puts payments on the topic, keyed by payment id.
 *
 * Keying matters twice over here. It decides which partition an event lands
 * in, and it is the identity the idempotency claim is later made on. The
 * demo would still rebalance without a key, but the duplicate would be harder
 * to attribute to a specific payment, and attribution is the whole point.
 */
@Component
public class PaymentProducer {

    static final String TOPIC = "payments";

    private final KafkaTemplate<String, String> kafka;

    public PaymentProducer(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    public void send(PaymentEvent event) {
        kafka.send(TOPIC, event.paymentId(), event.serialised());
    }

    public void sendPayments(int count) {
        for (int i = 1; i <= count; i++) {
            send(PaymentEvent.initiated("payment-" + i, 10_000L + i));
        }
        kafka.flush();
    }
}
