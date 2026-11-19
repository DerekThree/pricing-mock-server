# pricing-mock-server

Spring Boot Mock Server. The Mock Account Processor accepts Pricing Batches at
`POST /api/v1/mock/acc/batch`, storing the complete submission and pending Batch Outbox row in one
transaction before returning `202` with `batchId` and an acceptance message.

The Transaction Processor wake-up endpoint is `POST /api/v1/mock/txn/wake`.
Both processors share the `/api/v1/mock/*` routing prefix.

The controller delegates to `AccountService`, which validates submissions and owns the transaction.
Existing Account Store entities and value conversion classes are grouped in `accountStore`,
matching the Engine's package convention. Recommendation mappings live in `recommendationStore`.
The base package is `com.pricing.mockServer`; Account Processor handling classes live in
`accountProcessor`, and Transaction Processor handling classes live in `transactionProcessor`.
`AccountRepository` persists the batch with its owned accounts, attributes, and Fee Requests through
JPA cascades, following the backend's owned-child mappings.
Batch identity uses insert-only persistence and the PostgreSQL outbox primary key. A repeated
`batchId` returns `409` with code `BATCH_ALREADY_ACCEPTED` and a message identifying the batch,
regardless of payload differences. Concurrent requests can accept only one batch for that ID;
rejected transactions preserve all accepted rows, including the outbox's `processed` flag.

`BATCH_MAX_ACCOUNTS` sets the maximum Accounts per Pricing Batch (default `100000`).
Set it before starting the server; no submission contract change is required.
Submissions above this limit return `400 INVALID_BATCH` with the configured limit and
submitted count in the message, before any rows are stored.

## API contract ownership and compatibility

The Mock Server owns `openapi.yaml` (currently API version `1.0.0`) and
publishes it at `/openapi.yaml`. The Dashboard is a consumer: it generates its API client
from this provider-owned contract rather than independently defining request or response
models. The path `POST /api/v1/mock/acc/batch` is the supported v1 submission contract.

- **Compatible v1 changes:** documentation clarifications, or additive optional request
  fields whose absence preserves existing behavior. An optional response field may also be
  compatible if the current generated Dashboard client tolerates unknown fields. Compatibility
  must be verified against consumers; adding a field is not automatically safe.
- **Breaking changes:** renaming or removing `BatchRequest.batchId` or `accounts`, changing
  their types, making optional fields required, narrowing accepted values, renaming the
  required `BatchAcceptance.batchId` or `message`, changing the `202` acceptance status,
  or removing or changing the `409 BATCH_ALREADY_ACCEPTED` error contract. These require a
  new major API version rather than changing v1 in place.
- **Versioned migration:** introduce breaking changes in a separately documented v2
  endpoint/contract (for example, `/api/v2/mock/acc/batch`; this endpoint does not exist
  yet). Keep the existing v1 endpoint and semantics available while Dashboard and any other
  clients migrate. Announce the contract differences and migration steps to consumers,
  deploy the new provider version before switching clients, and regenerate clients against
  the intended version. Deprecate and retire v1 only after known consumers have migrated,
  the transition has been communicated, and the agreed support window has elapsed.
- **Release checks:** review an OpenAPI diff for compatibility before merging any provider
  contract change. Generate and compile the provider interfaces, run its PostgreSQL-backed
  API tests, regenerate and compile the Dashboard client, and validate its submission flow
  against the targeted API version. Include the `202` acceptance and `409` duplicate-batch
  behavior. During a v2 migration, keep regression tests for v1 until v1 is retired.

## Transaction Processor Transaction Batches

The Transaction Processor runs in this Mock Server alongside the Account Processor.
`POST /api/v1/mock/txn/wake` is bodyless and returns an empty `202` before processing completes.
On startup or restart and each wake-up, it drains pending Recommendation Outbox batches serially.
For each batch, it maps stored recommendations into one complete `TransactionBatch`. It attempts
live delivery once to the connected Dashboard at `/api/v1/mock/txn/batches`, or skips delivery
if the Dashboard is disconnected. It sets `processed` true after the attempt or skip, including
when the socket write fails. It does not recalculate pricing or execute financial transactions.
Recommendation rows are retained unchanged.

`asyncapi.yaml` owns the WebSocket contract and references `schemas/transaction-batch-v1.json`.
Both are published as static resources. The Dashboard generates its message models from this
schema; persistence entities and Engine executable classes are not the wire contract.

Configure `RECOMMENDATION_STORE_URL`, `RECOMMENDATION_STORE_USERNAME`, and
`RECOMMENDATION_STORE_PASSWORD`, following the Engine's environment naming. Defaults target
`jdbc:postgresql://localhost:5432/recommendationdb` with the existing
`recommendationdb_txn_processor` login and local password `recommendationpass`.
The Engine owns migrations and grants; this consumer validates the schema without migrating it.
Its role reads recommendations and updates only the Recommendation Outbox.

Startup and HTTP wake-ups share the existing synchronized drain. Overlapping or duplicate
wake-ups cannot advance to another batch before the current batch's completion update commits
or resend already-processed rows. Reconnecting only restores delivery of future results. Database
recovery remains in ticket 05.
A socket send and the completion update are not atomic: a crash between them leaves the batch
pending and can cause delivery again when processing is resumed. Delivery is not exactly-once.
Production uses the existing ECS/ALB deployment; deployed verification remains in ticket 06.

## Pricing Engine wake-ups

After transactional `AccountRepository.save` returns with the transaction committed,
`AccountService.accept` starts `notifyPricingEngine` on a Java virtual thread. The method sends
an empty HTTP `POST` to the configured Pricing Engine URL, with no Batch ID or business payload.
The submission returns `202` without waiting for the consumer or delivery retries.

| Configuration variable | Default | Purpose |
| --- | --- | --- |
| `ENGINE_WAKE_UP_URL` | `http://localhost:8082/api/v1/work-available` | Consumer endpoint |
| `PRICING_ENGINE_MAX_ATTEMPTS` | `3` | Total requests, including the initial attempt |
| `PRICING_ENGINE_HTTP_TIMEOUT` | `2s` | Connection and response-read timeout |
| `PRICING_ENGINE_RETRY_DELAY` | `1s` | Delay between failed attempts |

Any `2xx` response ends the sequence. Other statuses, connection errors, and timeouts consume
an attempt. Retries remain in memory and stop after the configured total attempt limit.
Delivery failure does not become `ACCOUNT_STORE_UNAVAILABLE` or change accepted rows or
`processed`. No notification-delivery state is persisted.

On startup or restart, `AccountService` checks through `AccountRepository` for unprocessed Batch
Outbox rows once the application is ready. If any exist, it starts the same bounded HTTP delivery
on a virtual thread, using the configured endpoint, timeouts, retry delay, and attempt limit.
An empty store or only processed rows produces no recovery wake-up. Recovery leaves all stored
rows unchanged, performs no continuous polling, and can send duplicate wake-ups on later restarts.

Delivery logs use `INFO` for an acknowledged wake-up, `WARN` for failed attempts with retries
remaining or an interrupted retry, and `ERROR` for the final failed attempt. Failed attempts
include the HTTP status or exception reason; retry warnings include the delay.

## Local database

Use a PostgreSQL database separate from the Pricing Store.
The main repository's `deploy/local/compose.yaml` hosts both databases in one PostgreSQL container
and starts all four application containers. Run `deploy/setup.ps1` from that repository for local
or AWS startup. Local passwords are stored in `deploy/local/.env`; `ACCOUNT_PASSWORD` is used by
this server. The shared initialization script creates `accountdb` and `accountuser` before startup.

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
interfaces and models in `com.pricing.mockServer.generated`, independently of the backend.

The Dockerfile follows the Backend's Java 21 multistage Maven build. `GET /actuator/health`
reports application/database health without exposing internal details. Compose and AWS use
this endpoint for health checks. In AWS, a one-off Fargate task initializes the Account Store
on the existing RDS instance before this service starts; ECS supplies its password from Secrets Manager.

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
