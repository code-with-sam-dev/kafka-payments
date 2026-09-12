# Episode 3: Kafka Rebalancing

Every consumer healthy. The dashboard green. The lag climbing anyway.

This module makes that happen on purpose, in one command, so you can watch it
instead of taking anyone's word for it.

**Episode:** https://youtu.be/VKOBY2vv1K0
**Written version:** https://code-with-sam-dev.github.io/blog/kafka-rebalancing-explained/
**Design sheet:** https://code-with-sam-dev.github.io/downloads/kafka-rebalancing-design-sheet.pdf

## Run it

```bash
docker compose up --build
```

Then, in another terminal:

```bash
curl -X POST "http://localhost:8081/demo/run?payments=300"
# wait a few seconds, and watch the app logs
curl -s http://localhost:8081/demo/results | jq
```

Episode 1 uses ports 29092 and 8080, so this one uses 29093 and 8081 and the
two can run side by side.

## What you should see

In the logs, an eviction happening while work is in flight:

```
ASSIGNED  [payments-0, payments-1, payments-2, payments-3, payments-4, payments-5]
REVOKED BEFORE COMMIT  [payments-0, ...]  <- anything processed and not committed will repeat
ASSIGNED  [payments-3, payments-4, payments-5]
ASSIGNED  [payments-0, payments-1, payments-2]
```

And in the results, the two numbers the whole episode is about:

```json
{
  "paymentsSeen": 200,
  "effectsPerformed": 200,
  "rebalances": 5,
  "revocationsBeforeCommit": 3,
  "redeliveredPayments": 200,
  "duplicatedEffects": {},
  "verdict": "Records were delivered more than once. The effect ran exactly once."
}
```

Every payment was delivered more than once. Not one effect ran twice.

## Why the consumer was evicted

Not because it died. Because it was SLOW.

The heartbeat runs on its own background thread and reported the process alive
the entire time. What breached the limit was `max.poll.interval.ms`: the time
between calls to `poll`. The handler spent longer than that on one batch, so
the group coordinator treated the member as failed and reassigned its
partitions.

The log line says the member left the group, which reads exactly like a crash
and is not one. That is why the usual fix is to do less work per poll rather
than to raise the timeout.

## Where the duplicate comes from

Follow it through:

1. The consumer reads a batch.
2. It processes the batch.
3. Before it commits, it is evicted.
4. The partition is reassigned.
5. The new owner starts from the last committed offset, which is **before**
   that batch.
6. It processes the batch again.

This is not a bug and it is not a race you can tune away. It is at-least-once
delivery plus a reassignment landing between processing and committing.

## The only real defence

`PaymentLedger.applyOnce` claims the payment id before performing the effect.
The claim is atomic, so a redelivery, or two members briefly overlapping on the
same partition during a handover, still yields exactly one effect.

In a real system that claim is a unique constraint in a database, not a set in
memory. The shape and the reasoning are identical: the defence is that the
**work** refuses to happen twice, never that the delivery promises not to
arrive twice.

## The settings that force the failure

In `application.yml`:

| Setting | Value | Why |
|---|---|---|
| `max.poll.interval.ms` | 8000 | Short, so a slow batch breaches it fast |
| `max.poll.records` | 100 | Large enough that one poll holds real work |
| `demo.processing-millis` | 120 | 100 x 120 ms = 12s against an 8s limit |
| `demo.concurrency` | 2 | Two members, so a revocation has somewhere to go |
| `session.timeout.ms` | 45000 | Long, so heartbeating stays healthy throughout |

**None of this is advice.** These are hostile values chosen to make an eviction
happen in seconds so it can be watched. In production you turn them the other
way. Override any of them with the environment variables named beside them in
`application.yml`.

## Tests

```bash
mvn test
```

The tests cover the property the episode rests on: the effect runs once
however many times the record is delivered, including under the concurrency a
rebalance actually produces. They need no broker, deliberately, because the
property is about the side effect rather than about Kafka.

## Verified

Claims checked against Apache Kafka 4.3 documentation on 2026-09-12. The demo
itself runs `apache/kafka:3.8.0`, which is what the compose file pins.
