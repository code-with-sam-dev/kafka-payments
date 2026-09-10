package com.codewithsam.ordering;

/**
 * One step in a payment's lifecycle.
 *
 * @param paymentId the business entity whose ordering must be preserved; this
 *                  is also used as the Kafka message key, which is what routes
 *                  every event for one payment to the same partition
 * @param type      INITIATED, AUTHORIZED or COMPLETED
 * @param sequence  the position this event SHOULD occupy, carried purely so the
 *                  demo can prove whether ordering survived
 */
public record PaymentEvent(String paymentId, String type, int sequence) {

    @Override
    public String toString() {
        return paymentId + " " + type + " (#" + sequence + ")";
    }
}
