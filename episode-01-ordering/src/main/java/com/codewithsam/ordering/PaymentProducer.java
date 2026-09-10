package com.codewithsam.ordering;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Produces payment events.
 *
 * The important line is the send call: passing paymentId as the KEY is what
 * makes Kafka route every event for one payment to the same partition, and a
 * partition is the only place Kafka guarantees ordering.
 */
@Component
public class PaymentProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentProducer.class);

    public static final String TOPIC = "payment-events";

    private final KafkaTemplate<String, String> kafka;

    public PaymentProducer(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    /** Sends one payment's three events, in the correct order. */
    public void sendLifecycle(String paymentId) {
        send(new PaymentEvent(paymentId, "INITIATED", 1));
        send(new PaymentEvent(paymentId, "AUTHORIZED", 2));
        send(new PaymentEvent(paymentId, "COMPLETED", 3));
    }

    private void send(PaymentEvent event) {
        // paymentId as the key == same partition == ordering preserved on the wire.
        kafka.send(TOPIC, event.paymentId(), serialise(event));
        log.info("produced  {}", event);
    }

    /** Deliberately trivial so the demo has no serialisation dependency. */
    private String serialise(PaymentEvent e) {
        return e.paymentId() + "," + e.type() + "," + e.sequence();
    }

    static PaymentEvent deserialise(String raw) {
        String[] parts = raw.split(",");
        return new PaymentEvent(parts[0], parts[1], Integer.parseInt(parts[2]));
    }
}
