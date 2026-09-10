package com.codewithsam.ordering;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka ordering, demonstrated rather than asserted.
 *
 * The demo produces the three events of a payment lifecycle, in order, and
 * consumes them twice:
 *
 *   BROKEN     the consumer hands each record to a thread pool, which is the
 *              single most common way teams destroy ordering that Kafka
 *              delivered perfectly
 *   CORRECT    the consumer processes records sequentially per partition
 *
 * Same topic, same records, same order on the wire. Different output. That
 * difference is the whole lesson.
 */
@SpringBootApplication
public class OrderingDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderingDemoApplication.class, args);
    }
}
