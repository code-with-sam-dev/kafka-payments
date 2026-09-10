package com.codewithsam.ordering;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * The claim, asserted rather than described.
 *
 * Runs against an embedded broker, so `mvn test` needs no Docker and no
 * network. The compose setup is for the interactive demo; this is for CI.
 *
 * Note what is NOT under test: Kafka. Kafka's delivery order is correct in both
 * cases. What these tests pin down is that the APPLICATION either preserves or
 * destroys that order, which is the entire point of the episode.
 *
 * Isolation note, learned the hard way: both consumers subscribe to the same
 * topic and drain at different speeds, so when one test finishes the other
 * consumer is often still working through the same records. Test methods
 * therefore use UNIQUE payment ids and assert only on their own. Clearing
 * shared state while async consumers are still live is a race, not isolation.
 */
@SpringBootTest
@EmbeddedKafka(
    partitions = 3,
    topics = {PaymentProducer.TOPIC},
    bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OrderingGuaranteeTest {

    @Autowired PaymentProducer producer;
    @Autowired ProcessingLog processingLog;

    @Test
    @DisplayName("sequential consumer preserves the order Kafka delivered")
    void correctConsumerPreservesOrder() {
        List<String> ids = uniquePaymentIds(2);
        ids.forEach(producer::sendLifecycle);

        awaitProcessed(CorrectConsumer.STRATEGY, ids);

        assertThat(processingLog.isOrderedFor(CorrectConsumer.STRATEGY, ids))
            .as("processing on the listener thread keeps per-key order intact")
            .isTrue();
    }

    @Test
    @DisplayName("every event still arrives, even when the consumer reorders them")
    void brokenConsumerStillReceivesEverything() {
        List<String> ids = uniquePaymentIds(2);
        ids.forEach(producer::sendLifecycle);

        awaitProcessed(BrokenConsumer.STRATEGY, ids);

        // Deliberately asserting delivery, NOT order. The broken consumer loses
        // nothing, which is exactly why the bug survives review: counts
        // reconcile, logs look healthy, and the resulting state is still wrong.
        assertThat(processingLog.countFor(BrokenConsumer.STRATEGY, ids)).isEqualTo(6);
    }

    /** Unique per test method, so leftovers from another test cannot bleed in. */
    private List<String> uniquePaymentIds(int count) {
        String run = UUID.randomUUID().toString().substring(0, 8);
        return IntStream.rangeClosed(1, count)
            .mapToObj(i -> "payment-" + run + "-" + i)
            .toList();
    }

    private void awaitProcessed(String strategy, List<String> ids) {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(100))
            .until(() -> processingLog.countFor(strategy, ids) >= ids.size() * 3L);
    }
}
