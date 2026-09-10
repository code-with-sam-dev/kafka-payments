# Contributing

This repository backs a video series, so it has one unusual constraint worth
knowing before you open a pull request.

## The incremental rule

Each stage is the previous stage plus **exactly one** new concept. That is the
whole point: a viewer can diff `episode-02` against `episode-01` and see only
the thing the episode was about.

So a change that improves `episode-01` by introducing something episode 1 has
not taught yet will be declined, however good the change is. It is not a
judgement on the code. It would just break the diff that makes the series
readable.

## What is very welcome

- Bugs, especially anything that makes a stage fail to start on a clean machine
- Correctness problems in how Kafka behaviour is demonstrated
- Flaky tests, and races in the tests themselves
- Documentation that was wrong, unclear, or assumed knowledge it should not
- Support for an environment that does not currently work

## Running it

You need Docker and nothing else. A JDK is not required, the build runs in a
container.

```bash
cd episode-01-ordering
docker compose up --build
```

Then, in another terminal:

```bash
curl -X POST localhost:8080/demo/run
curl localhost:8080/demo/results
```

## Running the tests

```bash
cd episode-01-ordering
./mvnw test
```

Tests are written before the code they cover, and each test method uses its own
unique payment identifiers so that stages sharing a topic cannot interfere with
each other. If you add a test, keep that property. A test that passes only when
run alone is worse than no test.

## Before you open a pull request

- `./mvnw test` passes on the stage you touched
- The stage still starts from clean with `docker compose up --build`
- One concept per pull request. Two good ideas are two pull requests
- Commit messages say why, not what. The diff already says what

CI runs every stage independently, so a change that breaks a later stage is
caught even if the stage you edited is fine.
