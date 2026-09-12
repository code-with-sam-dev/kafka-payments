package com.codewithsam.rebalancing;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The demo's interface: a REST endpoint rather than a UI.
 *
 * A UI would be more work, more to maintain, and would obscure the point. An
 * endpoint you can hit with curl keeps the demo about Kafka, and it is what a
 * backend engineer would actually ship.
 *
 *   POST /demo/run?payments=200   produce payments and let the slow consumer
 *                                 be evicted while processing them
 *   GET  /demo/results            deliveries against effects, and the
 *                                 rebalances that explain the difference
 *   POST /demo/reset              clear everything and start again
 */
@RestController
public class DemoController {

    private final PaymentProducer producer;
    private final ProcessingLog log;
    private final PaymentLedger ledger;

    public DemoController(PaymentProducer producer, ProcessingLog log, PaymentLedger ledger) {
        this.producer = producer;
        this.log = log;
        this.ledger = ledger;
    }

    @PostMapping("/demo/run")
    public Map<String, Object> run(@RequestParam(defaultValue = "200") int payments) {
        producer.sendPayments(payments);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("produced", payments);
        out.put("next", "GET /demo/results after a few seconds, and watch the logs for REVOKED BEFORE COMMIT");
        return out;
    }

    @GetMapping("/demo/results")
    public Map<String, Object> results() {
        Map<String, Integer> redelivered = log.redelivered();
        Map<String, Integer> duplicatedEffects = log.duplicatedEffects();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("paymentsSeen", log.allDeliveries().size());
        out.put("effectsPerformed", ledger.size());
        out.put("rebalances", log.rebalanceCount());
        out.put("revocationsBeforeCommit", log.evictionCount());
        out.put("redeliveredPayments", redelivered.size());
        out.put("redeliveredDetail", redelivered);

        // The assertion the episode makes, checked live rather than claimed.
        out.put("duplicatedEffects", duplicatedEffects);
        out.put(
            "verdict",
            duplicatedEffects.isEmpty()
                ? "Records were delivered more than once. The effect ran exactly once. That is idempotency doing its job."
                : "AN EFFECT RAN TWICE. The idempotency claim is broken, and this demo is telling you so.");
        return out;
    }

    @PostMapping("/demo/reset")
    public Map<String, Object> reset() {
        log.clear();
        ledger.clear();
        return Map.of("status", "cleared");
    }
}
