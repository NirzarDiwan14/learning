# Worked Example: Integration Events for a Food-Delivery App

Services: **orders**, **restaurants**, **payments**, **dispatch** (couriers), **notifications**, **analytics**.
All types are `com.fooddash.<entity>.<what_happened>.v1`, `subject` = the entity id (also the Kafka key).

## Happy path

| # | Event | Producer | Consumers | Thin / fat | `data` | Why this shape |
|---|---|---|---|---|---|---|
| 1 | `order.placed` | orders | restaurants, payments, dispatch, notifications, analytics | **fat** | order_id, customer_id, restaurant_id, items (dish_id, name, qty, price_cents), total_cents, currency, delivery_address, placed_at | 5 consumers: a thin event would mean 5 callbacks per order, and the orders service would become a bottleneck. Prices are *as of ordering*, so they must travel with the event |
| 2 | `payment.authorized` | payments | orders, restaurants | thin-ish | payment_id, order_id, amount_cents, currency | consumers only need "it's paid, and how much". **No card data, ever** |
| 3 | `order.accepted_by_restaurant` | restaurants | orders, dispatch, notifications | thin-ish | order_id, restaurant_id, estimated_ready_at | dispatch needs the ready time to schedule a courier |
| 4 | `courier.assigned` | dispatch | orders, notifications | fat | order_id, courier_id, courier_first_name, vehicle, eta | the customer app shows the courier's first name. Only what's shown, no phone number |
| 5 | `order.picked_up` | dispatch | orders, notifications, analytics | thin | order_id, courier_id, picked_up_at | it's just a status change |
| 6 | `order.delivered` | dispatch | orders, payments (capture), notifications, analytics | thin | order_id, delivered_at | payments captures the money it already authorized for that order |

## Failure paths

| # | Event | Producer | Consumers | Thin / fat | `data` | Why |
|---|---|---|---|---|---|---|
| 7 | `payment.declined` | payments | orders, notifications | thin | order_id, reason_code (`insufficient_funds`, `card_expired`, …) | a **code**, not the bank's raw message: it's a contract |
| 8 | `order.rejected_by_restaurant` | restaurants | orders, payments (void), notifications | thin | order_id, reason_code (`closed`, `item_unavailable`, `too_busy`) | |
| 9 | `dispatch.no_courier_available` | dispatch | orders, restaurants, notifications | thin | order_id, retries_attempted, given_up_at | |
| 10 | `order.cancelled` | orders | everyone involved | thin | order_id, cancelled_by (`customer` / `restaurant` / `system`), reason_code | one event for all cancel reasons, with a field. Compare with #8: a restaurant rejection *before* acceptance is a different business fact |

## Deliberately thin, and why
- **`order.delivered`**: every consumer already received the order details in `order.placed`. Repeating them would
  only create a second copy that could disagree.
- **Customer details**: no event carries the customer's email or phone. `notifications` looks them up from `customer_id`
  in its own copy of customer data (built from `customer.registered` / `customer.contact_details_changed`).
  That keeps personal data in **two** places instead of in every event log.

## What's NOT here
- `OrderUpdated`: replaced by specific events (#3–#6, #10).
- Commands like `AssignCourier`: those go to the owning service directly (Lesson 05).
- Internal events like `restaurants`' `TicketPrinted`: domain events that stay inside one service.
