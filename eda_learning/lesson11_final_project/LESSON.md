# Lesson 11: Final Project (and When NOT to Use EDA)

**Goal:** design an event-driven system from a blank page, build it, break it on purpose, and defend your choices,
including the places where you chose **not** to use events.

## Part 1: When not to use EDA (read this first)

Events add real costs: eventual consistency, harder debugging, duplicate handling, schema governance, more
infrastructure. They're worth it only when you need what they give you.

**Prefer a direct call (or a plain monolith) when:**
- the caller **needs the answer now** to continue (validation, price lookup, "is this seat free?")
- the flow is simple CRUD with one team and one database
- you need **strong consistency** across the data involved: put it in one service and one transaction
- the team is small and the system is young: a well-structured monolith that publishes a few events is often the best start

**Anti-patterns to recognize:**
| Anti-pattern | What it looks like |
|---|---|
| **Distributed monolith** | services that can't be deployed or run without each other, now with a broker in between |
| **Event spaghetti / pinball** | nobody can explain the flow. Events trigger events that trigger events |
| **Passive-aggressive events** | an "event" that's really a command: `OrderNeedsShipping` that only shipping must act on, and the publisher breaks if it doesn't |
| **Request/response over events** | publish `GetCustomer`, wait for `CustomerReturned`. That's a slow, fragile HTTP call |
| **Entity-CRUD events** | `CustomerUpdated` with the whole row, every time (Lesson 02) |
| **Shared database behind the events** | services "decoupled" by events, but all reading the same tables |

## Part 2: Designing with Event Storming

**Event Storming** (Alberto Brandolini) is a workshop technique for discovering a domain's events. Do it on paper,
Miro or a markdown table, using the sticky-note colors:

| Color | Meaning |
|---|---|
| 🟧 orange | **domain event** (past tense): `TicketReserved` |
| 🟦 blue | **command** that causes it: `ReserveTicket` |
| 🟨 yellow | **aggregate** that decides: `Screening` |
| 🟪 lilac | **policy**: "whenever X, then Y" (`when PaymentFailed → ReleaseSeats`) |
| 🟩 green | **read model** a user needs to decide |
| 🩷 pink | **external system**: payment provider, email |
| 🟥 red | **hotspot**: open question, risk, disagreement |

Steps: put every event on a timeline → add commands and actors → group into aggregates → draw **bounded contexts**
(these become services) → mark which events cross a boundary (those are your **integration events**).

## Part 3: The project

> **Note:** the spec below is the plan. When you start this lesson, Claude writes and tests the code and replaces
> this section with **Files** + **Experiments** for you to run (like Lesson 01).

**Domain: a cinema ticket booking system.** (Choose another domain if you prefer, with similar complexity.)

Core flow: a customer picks a screening and seats → the seats are held for 10 minutes → payment → tickets issued
(QR code) → confirmation email. Also: holds expire, payment fails, a screening is cancelled (refund everyone),
and a customer cancels up to 2 hours before the show.

### Requirements: use each lesson at least once
| Lesson | Requirement |
|---|---|
| 01 | at least one place where you **deliberately use a direct call** instead of an event, justified in an ADR |
| 02 | CloudEvents envelope for all integration events; one thin and one fat event, justified |
| 03 | every consumer idempotent; one consumer that handles out-of-order events with versions |
| 04 | outbox in every service that publishes, inbox in every service that consumes |
| 05 | the booking flow **orchestrated**, notifications **choreographed** |
| 06 | the booking saga with compensations and the 10-minute **timeout** |
| 07 | the `Screening` (seats) aggregate **event-sourced**, with optimistic concurrency (two customers grab the same seat!) |
| 08 | at least two projections: "seat map for a screening" and "my tickets", with read-your-own-writes on one |
| 09 | `TicketIssued` evolves from v1 to v2 mid-project, with an upcaster and a compatibility test |
| 10 | correlation/causation ids everywhere, and a `trace.py` for any booking |

Tech: Python, Kafka (from the repo's docker-compose), SQLite or Postgres, FastAPI for the HTTP edge.

### Deliverables (in this folder)
1. `EVENT_STORMING.md`: the board, the bounded contexts, the list of integration events
2. `adr/`: Architecture Decision Records (one short file per decision: context, decision, consequences). At least 5:
   orchestration vs choreography, where events are *not* used, thin vs fat, event store choice, consistency strategy for the seat map
3. `asyncapi.yaml` for all integration events
4. the code, one folder per service, plus a `README.md` with how to run it
5. `chaos.py`: a script that runs many concurrent bookings while killing services at random, then checks the invariants:
   - no seat is ever sold twice
   - every payment either leads to tickets or is fully refunded
   - every hold is either converted or released within ~10 minutes
   - no event is lost: every booking reaches a final state
6. `REVIEW.md`: what you'd change for 1,000× the traffic, and what you'd remove if this were a 2-person startup

**Done when:** `chaos.py` passes, and you can explain every ADR to me without notes. Send me the repo layout and the
ADRs first. I'll review the design before you write much code.

## Questions (answer them in your own words, then send them to me)

1. Which parts of your design use direct calls, and why are events wrong there?
2. Two customers click the same seat at the same moment. Walk through exactly what happens in your system.
3. What's the worst thing that can happen if your outbox relay is down for an hour? What do customers see?
4. Which of your events would be hardest to change later, and why?
5. If you had to remove half of the patterns to ship in two weeks, which would you drop and which would you keep?
6. Explain to a non-technical manager, in 5 sentences, why the seat map may be a second out of date and why that's fine.
