# pricing-mock-server

Spring Boot Mock Server. The Mock Account Processor accepts Pricing Batches at
`POST /api/v1/mock/acc/batch`, storing the complete submission and pending Batch Outbox row in one
transaction before returning `202` with `batchId` and an acceptance message.

The planned Transaction Processor wake-up endpoint is `POST /api/v1/mock/txn/wake`.
Both paths share the `/api/v1/mock/*` routing prefix. The wake-up endpoint is not implemented yet.

The controller delegates to `BatchService`, which validates submissions and owns the transaction.
`BatchRepository` persists the batch with its owned accounts, attributes, and Fee Requests through
JPA cascades, following the backend's owned-child mappings.
Batch identity uses insert-only persistence and the PostgreSQL outbox primary key. A repeated
`batchId` returns `409` with code `BATCH_ALREADY_ACCEPTED` and a message identifying the batch,
regardless of payload differences. Concurrent requests can accept only one batch for that ID;
rejected transactions preserve all accepted rows, including the outbox's `processed` flag.

`BATCH_MAX_ACCOUNTS` sets the maximum Accounts per Pricing Batch (default `100000`).
Set it before starting the server; no submission contract change is required.
Submissions above this limit return `400 INVALID_BATCH` with the configured limit and
submitted count in the message, before any rows are stored.

## Pricing Engine wake-ups

After transactional `BatchRepository.save` returns with the transaction committed,
`BatchService.accept` starts `notifyPricingEngine` on a Java virtual thread. The method sends
an empty HTTP `POST` to the configured Pricing Engine URL, with no Batch ID or business payload.
The submission returns `202` without waiting for the consumer or delivery retries.

| Configuration variable | Default | Purpose |
| --- | --- | --- |
| `PRICING_ENGINE_WAKE_UP_URL` | `http://localhost:8082/api/v1/work-available` | Consumer endpoint |
| `PRICING_ENGINE_MAX_ATTEMPTS` | `3` | Total requests, including the initial attempt |
| `PRICING_ENGINE_HTTP_TIMEOUT` | `2s` | Connection and response-read timeout |
| `PRICING_ENGINE_RETRY_DELAY` | `1s` | Delay between failed attempts |

Any `2xx` response ends the sequence. Other statuses, connection errors, and timeouts consume
an attempt. Retries remain in memory and stop after the configured total attempt limit.
Delivery failure does not become `ACCOUNT_STORE_UNAVAILABLE` or change accepted rows or
`processed`. No notification-delivery state is persisted.

On startup or restart, `BatchService` checks through `BatchRepository` for unprocessed Batch
Outbox rows once the application is ready. If any exist, it starts the same bounded HTTP delivery
on a virtual thread, using the configured endpoint, timeouts, retry delay, and attempt limit.
An empty store or only processed rows produces no recovery wake-up. Recovery leaves all stored
rows unchanged, performs no continuous polling, and can send duplicate wake-ups on later restarts.

Delivery logs use `INFO` for an acknowledged wake-up, `WARN` for failed attempts with retries
remaining or an interrupted retry, and `ERROR` for the final failed attempt. Failed attempts
include the HTTP status or exception reason; retry warnings include the delay.

## Local database

Use a PostgreSQL database separate from the Pricing Backend's Pricing Configuration database.
For local development, an administrator can create its application login and database:

```sql
create role accountuser login password 'accountpass';
create database accountdb owner accountuser;
```

The application login owns the database and objects it creates. It needs schema usage and
creation, table read/insert access, sequence access for generated identities, and permission
to change its schema through migrations. Flyway and runtime connections share this login;
there is no separate migration user. These example credentials are local development defaults.

Configure `SPRING_DATASOURCE_URL`, `SPRING_DATASOURCE_USERNAME`, and
`SPRING_DATASOURCE_PASSWORD` for another Account Store. Defaults are
`jdbc:postgresql://localhost:5432/accountdb`, `accountuser`, and `accountpass`.
Flyway creates the schema; Hibernate validates it and does not generate it at runtime.

From this directory:

```powershell
.\mvnw.cmd spring-boot:run
```

The default port is `8081` (`SERVER_PORT` overrides it). The provider-owned contract is
`openapi.yaml`, served at `/openapi.yaml`, with Swagger UI at `/docs`. Maven generates API
interfaces and models in `com.pricing.mockserver.generated`, independently of the backend.

## Account Store

`batch_outbox` contains only the submitted UUID `batch_id` and `processed`, initially false.
`accounts`, `account_attributes`, and `fee_requests` use generated bigint identity primary keys.
Account numbers are unique within a batch; submitted Fee Request IDs are unique within an
account. Attributes preserve repeated codes and store one JSONB string, number, or boolean.
Numeric-looking strings retain their type; numbers round-trip as `BigDecimal`.
Optional transaction amounts remain SQL null when absent; supplied amounts must be nonnegative
and use increments of `0.01`. Foreign keys preserve associations without ordering columns.

Unavailable Account Store connections return `503` with code `ACCOUNT_STORE_UNAVAILABLE`.
Failed transactions leave no partial batch and produce no wake-up. Pricing Engine evaluation and
processing updates remain outside this server; the synchronous Pricing Backend endpoint remains
available.

## Verification

Docker and Java 21 are required:

```powershell
.\mvnw.cmd clean test
```

The PostgreSQL 16 Testcontainers API suite uses dynamic datasource configuration, Flyway, and an
ordinary application login for migration and runtime access. It checks committed rows, scalar
round trips, generated keys, constraints, rollback, unavailable connections, unchanged work after
exact and changed-payload duplicates, and competing submissions for the same Batch ID.
Container tests follow the backend lifecycle and skip when Docker is absent; skipped tests are
not PostgreSQL validation. There is no H2 fallback.

Capacity tests override `BATCH_MAX_ACCOUNTS` to `5000` and check acceptance at the limit,
rejection at `5001`, and rollback after a final-account write failure.

`BatchNotificationApiTests` uses the real HTTP client against a local recording consumer, with
PostgreSQL reads inside the consumer proving commit visibility. It checks empty POSTs, rejection
and commit-failure suppression, acceptance before the consumer responds, immediate success,
transient and persistent failures, configured attempt limits, retry delays, response timeouts,
and unchanged accepted rows and processed flags. Restart tests close and reopen application
contexts against retained PostgreSQL data, including after exhausted delivery, and verify no
recovery for empty or processed-only stores. Both API suites use local HTTP consumers.
