package com.codewithsam.ordering;

import java.util.LinkedHashMap;
import java.util.List;
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
 *   POST /demo/run?payments=3     produce events and process them both ways
 *   GET  /demo/results            what each consumer strategy actually did
 *   POST /demo/reset              clear the log and start again
 */
@RestController
public class DemoController {

    private final PaymentProducer producer;
    private final ProcessingLog processingLog;

    public DemoController(PaymentProducer producer, ProcessingLog processingLog) {
        this.producer = producer;
        this.processingLog = processingLog;
    }

    /**
     * Produces the full lifecycle for N payments. Both consumers are listening,
     * so each receives the identical, correctly-ordered stream - which is what
     * makes the difference in their output attributable to them and not to Kafka.
     */
    @PostMapping("/demo/run")
    public Map<String, Object> run(@RequestParam(defaultValue = "3") int payments) {
        processingLog.clear();

        for (int i = 1; i <= payments; i++) {
            producer.sendLifecycle("payment-" + i);
        }

        return Map.of(
            "produced", payments * 3,
            "payments", payments,
            "next", "GET /demo/results in a second or two"
        );
    }

    /**
     * The payoff. Same input, two strategies, and `ordered` says plainly which
     * one preserved the guarantee.
     */
    @GetMapping("/demo/results")
    public Map<String, Object> results() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("broken", describe(BrokenConsumer.STRATEGY));
        out.put("correct", describe(CorrectConsumer.STRATEGY));
        out.put(
            "explanation",
            "Kafka delivered both consumers the same records in the same order. "
                + "The broken consumer handed them to a thread pool, so they "
                + "completed out of order. Ordering was lost in the application, "
                + "not in Kafka."
        );
        return out;
    }

    @PostMapping("/demo/reset")
    public Map<String, String> reset() {
        processingLog.clear();
        return Map.of("status", "cleared");
    }

    private Map<String, Object> describe(String strategy) {
        List<PaymentEvent> processed = processingLog.get(strategy);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ordered", processingLog.isOrdered(strategy));
        m.put("count", processed.size());
        m.put("processedInThisOrder", processed.stream().map(PaymentEvent::toString).toList());
        return m;
    }
}
