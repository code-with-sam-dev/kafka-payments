package com.codewithsam.rebalancing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * The configuration that makes an eviction happen on purpose.
 *
 * Three settings do all the work, and each is deliberately hostile:
 *
 *   max.poll.interval.ms   short, so a slow batch breaches it quickly
 *   max.poll.records       large enough that one poll holds real work
 *   concurrency            two members, so a revocation has somewhere to go
 *
 * A single member group would still be evicted, but with nowhere to reassign
 * the partitions to the duplicate is less obvious. Two members make the
 * handover visible.
 *
 * NONE OF THIS IS ADVICE. These are the values that force the failure in
 * seconds for a demo. In production the same knobs are turned the other way,
 * and the sheet says so.
 */
@Configuration
public class KafkaConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchFactory(
        ConsumerFactory<String, String> consumerFactory,
        RebalanceWatcher rebalanceWatcher,
        @Value("${demo.concurrency:2}") int concurrency) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceWatcher);
        return factory;
    }
}
