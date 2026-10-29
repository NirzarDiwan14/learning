# Event-Driven Architecture (EDA) Learning Roadmap

A hands-on course on **designing** event-driven systems, in Python.
The Kafka course taught you the *tool*. This course teaches the *architecture*: how to split a system into services
that talk through events, and how to keep it correct when things fail.

## How to do each lesson

1. **Read** the "The idea" section of the lesson's `LESSON.md`.
2. **Tell me you're ready for the lesson.** I write and test the code, and update the `LESSON.md` with the
   files and the experiments to run.
3. **Run the experiments** and watch what happens (break things on purpose, keep notes).
4. **Answer the questions** in your own words and send them to me. I give feedback, then add a
   "Questions & Answers (for revision)" section to the lesson.
5. Move on to the next lesson. Each one builds on the previous ones, so keep them in order.

**Setup:** Lessons 01–03 and 07–09 use only the Python standard library (+ `sqlite3`, `pydantic`), so the ideas aren't
hidden behind a broker. Lessons 04–06, 10 and 11 use Kafka (`docker compose up -d` from the repo root). Use topic names
starting with `eda-` so they don't mix with the Kafka course topics.
Run each lesson from its own folder with `uv run python <file>`.

| # | Lesson | Needs | Status |
|---|--------|-------|--------|
| [01](lesson01_why_events/LESSON.md) | Why events? Direct calls vs events, kinds of coupling, commands vs events vs queries | stdlib | Done |
| [02](lesson02_designing_events/LESSON.md) | Designing events: notification vs event-carried state transfer, thin vs fat, naming, the CloudEvents envelope | stdlib | In progress |
| [03](lesson03_delivery_and_idempotency/LESSON.md) | Delivery in the real world: queues vs pub/sub, duplicates, out-of-order events, idempotent consumers | stdlib + SQLite | |
| [04](lesson04_outbox_inbox/LESSON.md) | The dual-write problem: transactional outbox and inbox | Kafka + SQLite | |
| [05](lesson05_choreography_vs_orchestration/LESSON.md) | Choreography vs orchestration: who decides what happens next? | stdlib or Kafka | |
| [06](lesson06_sagas/LESSON.md) | Sagas: long-running workflows, compensating actions, timeouts | Kafka + SQLite | |
| [07](lesson07_event_sourcing/LESSON.md) | Event sourcing: events as the source of truth, optimistic concurrency, snapshots | SQLite | |
| [08](lesson08_cqrs_projections/LESSON.md) | CQRS and projections: read models, checkpoints, eventual consistency, read-your-own-writes | SQLite | |
| [09](lesson09_contracts_evolution/LESSON.md) | Contracts and evolution: compatibility, versioning, upcasting, AsyncAPI | stdlib + pydantic | |
| [10](lesson10_observability/LESSON.md) | Observability: correlation and causation IDs, structured logs, tracing, EDA metrics | Kafka | |
| [11](lesson11_final_project/LESSON.md) | Final project: a cinema booking system end to end, plus when NOT to use EDA | Kafka + FastAPI | |

## Recommended reading alongside the course
| Resource | Lessons |
|---|---|
| Martin Fowler, *"What do you mean by Event-Driven?"* (article / talk) | 01, 02 |
| Ben Stopford, *Designing Event-Driven Systems* (free e-book) | 01–08 |
| Chris Richardson, *Microservices Patterns* + microservices.io (outbox, saga, CQRS pages) | 04–08 |
| Martin Kleppmann, *Designing Data-Intensive Applications*, chapters 11–12 | 03, 07, 08 |
| cloudevents.io, asyncapi.com | 02, 09 |
| Alberto Brandolini, *Introducing EventStorming* | 11 |
