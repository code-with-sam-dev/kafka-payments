package com.codewithsam.ordering;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * The consumer that breaks ordering — and does it in the most reasonable-looking
 * way possible.
 *
 * Nothing here is obviously wrong. It polls a partition, receives records in the
 * correct order, and then hands each one to a thread pool "to go faster". That
 * is a change someone makes for performance, it passes review, and it works
 * under light load.
 *
 * Then the pool schedules them concurrently and they finish in whatever order
 * they finish. Kafka delivered A, B, C perfectly. The application applied them
 * as C, A, B.
 */
@Component
public class BrokenConsumer {

    private static final Logger log = LoggerFactory.getLogger(BrokenConsumer.class);

    public static final String STRATEGY = "broken";

    private final ExecutorService pool = Executors.newFixedThreadPool(4);
    private final ProcessingLog processingLog;

    public BrokenConsumer(ProcessingLog processingLog) {
        this.processingLog = processingLog;
    }

    @KafkaListener(topics = PaymentProducer.TOPIC, groupId = "broken-consumer")
    public void consume(String raw) {
        PaymentEvent event = PaymentProducer.deserialise(raw);

        // The bug. Ordering ends here, not in Kafka.
        pool.submit(() -> {
            sleepBriefly();
            processingLog.record(STRATEGY, event);
            log.info("broken   processed {}", event);
        });
    }

    /**
     * A little jitter so the race is reliably visible rather than occasionally
     * visible. Real work has variable duration; this just makes it obvious.
     */
    private void sleepBriefly() {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(20, 160));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
