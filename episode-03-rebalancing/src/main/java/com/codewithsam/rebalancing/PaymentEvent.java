package com.codewithsam.rebalancing;

/**
 * One payment, as it travels.
 *
 * The key is the payment id, which is what puts every event for one payment in
 * the same partition, and is also what the idempotency claim is made on. Those
 * being the same value is not a coincidence: the thing that decides ordering
 * and the thing that decides safety are the same business identity.
 */
public record PaymentEvent(String paymentId, String stage, long amountMinorUnits) {

    public static PaymentEvent initiated(String paymentId, long amountMinorUnits) {
        return new PaymentEvent(paymentId, "INITIATED", amountMinorUnits);
    }

    /** The wire format. Deliberately plain text: a demo should be greppable. */
    public String serialised() {
        return paymentId + "|" + stage + "|" + amountMinorUnits;
    }

    public static PaymentEvent parse(String raw) {
        String[] parts = raw.split("\\|", 3);
        return new PaymentEvent(parts[0], parts[1], Long.parseLong(parts[2]));
    }
}
