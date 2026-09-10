<img src="assets/images/series-banner.png" alt="Kafka Payments - Code with Sam" width="100%">

<p>
  <a href="https://github.com/code-with-sam-dev/kafka-payments/actions/workflows/ci.yml">
    <img src="https://github.com/code-with-sam-dev/kafka-payments/actions/workflows/ci.yml/badge.svg" alt="tests">
  </a>
  <img src="https://img.shields.io/badge/Java-21-orange" alt="Java 21">
  <img src="https://img.shields.io/badge/Spring%20Boot-3.3-brightgreen" alt="Spring Boot 3.3">
  <img src="https://img.shields.io/badge/Kafka-KRaft-black" alt="Kafka KRaft">
  <img src="https://img.shields.io/badge/licence-MIT-blue" alt="MIT licence">
  <a href="https://www.youtube.com/@CodewithSam-Dev">
    <img src="https://img.shields.io/badge/YouTube-Code%20with%20Sam-red?logo=youtube&logoColor=white" alt="YouTube">
  </a>
</p>

# Kafka Payments - build a production-grade service, one episode at a time

A payments service built incrementally across a video series. **Each episode
adds exactly one concept to the codebase**, and every stage runs on its own.

Start at episode 1 and follow along, or jump straight to the stage that covers
the thing you are trying to understand. By the last episode this is a service
you would not be embarrassed to run: idempotent producers, per-entity ordering,
consumer groups, retries and dead-letter handling, and full observability.

**[▶ Watch the series on Code with Sam](https://www.youtube.com/@CodewithSam-Dev)**

## The stages

| Stage | Episode | Adds | Status |
|-------|---------|------|--------|
| [`episode-01-ordering`](episode-01-ordering) | Kafka Ordering Explained | Partitions, message keys, and how consumer concurrency destroys ordering | ✅ |
| `episode-02-…` | - | - | planned |

> The remaining stages are being sequenced. Episode order is chosen for search
> demand **and** for a sensible build order - you cannot demonstrate retries and
> dead-letter topics before consumer groups exist.

## How to use this repo

Every stage is a complete, runnable project. Nothing is left as an exercise and
there are no `TODO` stubs.

```bash
git clone https://github.com/code-with-sam-dev/kafka-payments
cd kafka-payments/episode-01-ordering
docker compose up --build
```

You need Docker. Nothing else - no Java, no Kafka, no Maven installed locally.

Each stage exposes a small REST API rather than a UI, so you can drive it with
`curl` and keep your attention on the behaviour being demonstrated rather than
on a frontend.

### Running the tests

```bash
cd episode-01-ordering
mvn test
```

Tests run against an embedded broker, so they need neither Docker nor a
network. CI builds and tests **every stage independently**, which means a
change to a later episode can never silently break an earlier one - and that
matters, because each stage is what someone clones after watching that
particular episode.

## Why it is built this way

Most tutorial repositories are a snapshot: one concept, frozen, disconnected
from everything around it. You learn the concept and still cannot see how it
fits a real system.

Here the code accumulates. By the end you have watched a service grow from a
single producer into something with the failure handling and observability that
production actually demands - and every intermediate state is preserved so you
can see exactly what each idea changed.

## More from Code with Sam

Modern software engineering interview preparation for the AI era - the
questions senior backend engineers are actually asked, with a video, an
article, and runnable code for each.

**[▶ Subscribe on YouTube](https://www.youtube.com/@CodewithSam-Dev?sub_confirmation=1)**
· **[Read the articles](https://code-with-sam-dev.github.io)**

If this helped, a ⭐ makes it easier for the next person to find.

## Licence

MIT. Use it, fork it, take it into an interview.
