# pricing-mock-server

Spring Boot Mock Server. The Mock Account Processor accepts Pricing Batches at
`POST /api/v1/batch`, storing the complete submission and pending Batch Outbox row in one
transaction before returning `202` with `batchId` and an acceptance message.
The controller delegates to `BatchService`, which validates submissions and owns the transaction.
`BatchRepository` persists the batch with its owned accounts, attributes, and Fee Requests through
JPA cascades, following the backend's owned-child mappings.
Batch identity uses insert-only persistence and the PostgreSQL outbox primary key. A repeated
`batchId` returns `409` with code `BATCH_ALREADY_ACCEPTED` and a message identifying the batch,
regardless of payload differences. Concurrent requests can accept only one batch for that ID;
rejected transactions preserve all accepted rows, including the outbox's `processed` flag.

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
Failed transactions leave no partial batch. Pricing Engine evaluation, wake-ups, and processing
updates are outside this ticket; the synchronous Pricing Backend endpoint remains available.

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
