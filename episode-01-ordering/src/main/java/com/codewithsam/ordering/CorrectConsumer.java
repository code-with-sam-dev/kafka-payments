package com.codewithsam.ordering;

import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * The consumer that preserves ordering, by doing less rather than more.
 *
 * It processes each record on the listener thread, in sequence. One thread per
 * partition, work completed before the next record is taken.
 *
 * This is not slow: parallelism still exists, it just lives at the partition
 * level where Kafka intended it, instead of inside a single partition where it
 * destroys the guarantee. Want more throughput? Add partitions and consumers,
 * not threads inside one consumer.
 */
@Component
public class CorrectConsumer {

    private static final Logger log = LoggerFactory.getLogger(CorrectConsumer.class);

    public static final String STRATEGY = "correct";

    private final ProcessingLog processingLog;

    public CorrectConsumer(ProcessingLog processingLog) {
        this.processingLog = processingLog;
    }

    @KafkaListener(topics = PaymentProducer.TOPIC, groupId = "correct-consumer")
    public void consume(String raw) {
        PaymentEvent event = PaymentProducer.deserialise(raw);

        sleepBriefly();
        processingLog.record(STRATEGY, event);
        log.info("correct  processed {}", event);
    }

    /** Same variable work as the broken consumer, so the comparison is fair. */
    private void sleepBriefly() {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(20, 160));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
