package com.pricing.mockserver.batch;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.pricing.mockserver.accountattribute.AccountAttributeEntity;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers(disabledWithoutDocker = true)
class BatchApiTests {

	@Container
	private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
			.withDatabaseName("account_store")
			.withInitScript("db/account-store-test-role.sql");

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private EntityManager entityManager;

	@Autowired
	private HikariDataSource dataSource;

	@DynamicPropertySource
	static void configureProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
		registry.add("spring.datasource.username", () -> "account_app");
		registry.add("spring.datasource.password", () -> "account_test");
		registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
		registry.add("spring.datasource.hikari.connection-timeout", () -> "500");
		registry.add("spring.datasource.hikari.validation-timeout", () -> "250");
		registry.add("spring.datasource.hikari.maximum-pool-size", () -> "2");
		registry.add("spring.flyway.locations", () -> "classpath:db/migration");
	}

	@AfterEach
	void cleanUp() {
		jdbcTemplate.update("delete from fee_requests");
		jdbcTemplate.update("delete from account_attributes");
		jdbcTemplate.update("delete from accounts");
		jdbcTemplate.update("delete from batch_outbox");
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	void rejectsAcceptedIdentityWithoutChangingStoredWork(boolean changePayload) throws Exception {
		UUID batchId = UUID.randomUUID();
		String request = submission(batchId);
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content(request)).andExpect(status().isAccepted());
		jdbcTemplate.update("update batch_outbox set processed = true where batch_id = ?", batchId);
		List<List<Map<String, Object>>> accepted = storedWork();

		String repeated = changePayload ? request.replace("ACC001", "ACC002")
				.replace("PROD001", "PROD002").replace("BR001", "BR002")
				.replace("2026-10-21", "2026-10-22").replace("BALANCE", "CHANGED")
				.replace("12345678901234567890.123456789", "10.25")
				.replace("00123.40", "different").replace("true", "false")
				.replace("FEE001", "FEE003").replace("12.34", "56.78")
				.replace("\"feeRequestId\": 7", "\"feeRequestId\": 9") : request;
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content(repeated))
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.code").value("BATCH_ALREADY_ACCEPTED"))
				.andExpect(jsonPath("$.message", containsString("already been accepted")))
				.andExpect(jsonPath("$.message", containsString(batchId.toString())));

		assertEquals(accepted, storedWork());
		assertRowCounts(1, 1, 3, 2);
	}

	@Test
	void acceptsOneConcurrentSubmissionPerBatchIdentity() throws Exception {
		UUID sharedId = UUID.randomUUID();
		List<UUID> batchIds = List.of(sharedId, sharedId, sharedId, sharedId,
				UUID.randomUUID(), UUID.randomUUID());
		CountDownLatch ready = new CountDownLatch(batchIds.size());
		CountDownLatch start = new CountDownLatch(1);
		List<Future<MvcResult>> responses = new ArrayList<>();
		try (ExecutorService executor = Executors.newFixedThreadPool(batchIds.size())) {
			for (int index = 0; index < batchIds.size(); index++) {
				String request = submission(batchIds.get(index))
						.replace("ACC001", "ACC00" + index);
				responses.add(executor.submit(() -> {
					ready.countDown();
					assertTrue(start.await(10, TimeUnit.SECONDS));
					return mockMvc.perform(post("/api/v1/batch")
							.contentType(MediaType.APPLICATION_JSON).content(request)).andReturn();
				}));
			}
			assertTrue(ready.await(10, TimeUnit.SECONDS));
			start.countDown();

			int sharedAccepted = 0;
			for (int index = 0; index < responses.size(); index++) {
				MvcResult response = responses.get(index).get(30, TimeUnit.SECONDS);
				UUID batchId = batchIds.get(index);
				if (response.getResponse().getStatus() == 202) {
					jsonPath("$.batchId").value(batchId.toString()).match(response);
					jsonPath("$.message").value("Pricing Batch accepted").match(response);
					assertEquals("ACC00" + index, jdbcTemplate.queryForObject(
							"select account_number from accounts where batch_id = ?",
							String.class, batchId));
					if (batchId.equals(sharedId)) {
						sharedAccepted++;
					}
				} else {
					assertEquals(sharedId, batchId);
					status().isConflict().match(response);
					jsonPath("$.code").value("BATCH_ALREADY_ACCEPTED").match(response);
					jsonPath("$.message", containsString("already been accepted")).match(response);
					jsonPath("$.message", containsString(batchId.toString())).match(response);
				}
			}
			assertEquals(1, sharedAccepted);
		}
		assertRowCounts(3, 3, 9, 6);
		assertEquals(3, jdbcTemplate.queryForObject("""
				select count(*) from accounts account
				where (select count(*) from account_attributes
				       where account_id = account.id) = 3
				  and (select count(*) from fee_requests where account_id = account.id) = 2
				""", Integer.class));
		assertEquals(0, jdbcTemplate.queryForObject(
				"select count(*) from batch_outbox where processed", Integer.class));
	}

	@Test
	void acceptsCallerIdentityAfterStoringTheBatch() throws Exception {
		UUID batchId = UUID.randomUUID();
		mockMvc.perform(post("/api/v1/batch")
				.contentType(MediaType.APPLICATION_JSON)
				.content(submission(batchId)))
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.batchId").value(batchId.toString()))
				.andExpect(jsonPath("$.message").value("Pricing Batch accepted"));

		assertEquals(batchId, jdbcTemplate.queryForObject(
				"select batch_id from batch_outbox", UUID.class));
		assertFalse(jdbcTemplate.queryForObject(
				"select processed from batch_outbox", Boolean.class));
		assertRowCounts(1, 1, 3, 2);
		assertEquals("ACC001", jdbcTemplate.queryForObject(
				"select account_number from accounts where batch_id = ?", String.class, batchId));
		assertEquals("PROD001", jdbcTemplate.queryForObject(
				"select product_code from accounts", String.class));
		assertEquals("BR001", jdbcTemplate.queryForObject(
				"select branch_code from accounts", String.class));
		assertEquals(LocalDate.of(2026, 10, 21), jdbcTemplate.queryForObject(
				"select pricing_date from accounts", LocalDate.class));
		assertEquals(3, jdbcTemplate.queryForObject("""
				select count(*) from account_attributes attribute
				join accounts account on account.id = attribute.account_id
				where account.batch_id = ?
				""", Integer.class, batchId));
		assertEquals(2, jdbcTemplate.queryForObject(
				"select count(*) from account_attributes where code = 'BALANCE'", Integer.class));
		assertEquals(new BigDecimal("12.34"), jdbcTemplate.queryForObject(
				"select transaction_amount from fee_requests where fee_request_id = 7",
				BigDecimal.class));
		assertNull(jdbcTemplate.queryForObject(
				"select transaction_amount from fee_requests where fee_request_id = 8",
				BigDecimal.class));
		assertEquals(2, jdbcTemplate.queryForObject("""
				select count(*) from fee_requests fee
				join accounts account on account.id = fee.account_id
				where account.batch_id = ? and fee.id > 0 and fee.code in ('FEE001', 'FEE002')
				""", Integer.class, batchId));

		List<Object> values = entityManager.createQuery(
				"select attribute from AccountAttributeEntity attribute", AccountAttributeEntity.class)
				.getResultList().stream()
				.map(attribute -> attribute.getValue().getValue()).toList();
		assertTrue(values.contains("00123.40"));
		assertTrue(values.contains(Boolean.TRUE));
		assertTrue(values.stream().anyMatch(value -> value instanceof BigDecimal number
				&& number.compareTo(new BigDecimal("12345678901234567890.123456789")) == 0));
		assertEquals(List.of("boolean", "number", "string"), jdbcTemplate.queryForList(
				"select jsonb_typeof(value) from account_attributes order by jsonb_typeof(value)",
				String.class));
	}

	@Test
	void storesEveryAccountAndScopesSubmittedIdentitiesToTheirOwners() throws Exception {
		String accounts = """
				"accounts": [
				  {"accountNumber": "ACC001", "productCode": "PROD001", "branchCode": "BR001",
				   "pricingDate": "2026-10-21", "attributes": [],
				   "fees": [{"feeRequestId": 7, "code": "FEE001"}]},
				  {"accountNumber": "ACC002", "productCode": "PROD002", "branchCode": "BR002",
				   "pricingDate": "2026-10-22", "attributes": [],
				   "fees": [{"feeRequestId": 7, "code": "FEE002"}]}
				]
				""";
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content("{\"batchId\":\"" + UUID.randomUUID() + "\"," + accounts + "}"))
				.andExpect(status().isAccepted());
		assertRowCounts(1, 2, 0, 2);
		assertEquals(List.of("ACC001:FEE001", "ACC002:FEE002"), jdbcTemplate.queryForList("""
				select account.account_number || ':' || fee.code
				from accounts account join fee_requests fee on fee.account_id = account.id
				order by account.account_number
				""", String.class));
		assertEquals(2, jdbcTemplate.queryForObject(
				"select count(distinct id) from fee_requests where fee_request_id = 7", Integer.class));
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content("{\"batchId\":\"" + UUID.randomUUID() + "\"," + accounts + "}"))
				.andExpect(status().isAccepted());
		assertRowCounts(2, 4, 0, 4);
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content("{\"batchId\":\"" + UUID.randomUUID() + "\","
						+ accounts.replace("ACC002", "ACC001") + "}"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.message", containsString(
						"Account numbers must be unique within the Pricing Batch")))
				.andExpect(jsonPath("$.message", containsString("accounts[1].accountNumber")))
				.andExpect(jsonPath("$.message", containsString("accountNumber=ACC001")))
				.andExpect(jsonPath("$.message", containsString("accounts[0].accountNumber")));
		assertRowCounts(2, 4, 0, 4);
	}

	@Test
	void publishesTheProviderOwnedContract() throws Exception {
		mockMvc.perform(get("/openapi.yaml"))
				.andExpect(status().isOk())
				.andExpect(content().string(org.hamcrest.Matchers.containsString(
						"title: Mock Account Processor API")))
				.andExpect(content().string(containsString("'409':")))
				.andExpect(content().string(containsString("BATCH_ALREADY_ACCEPTED")));
	}

	@Test
	void initializesAndUsesTheSchemaWithOneOrdinaryApplicationRole() {
		assertEquals("account_app", jdbcTemplate.queryForObject("select current_user", String.class));
		assertFalse(jdbcTemplate.queryForObject(
				"select rolsuper from pg_roles where rolname = current_user", Boolean.class));
		assertEquals("account_app", jdbcTemplate.queryForObject("""
				select tableowner from pg_tables where tablename = 'flyway_schema_history'
				""", String.class));
		assertEquals(4, jdbcTemplate.queryForObject("""
				select count(*) from pg_tables where tableowner = current_user
				and tablename in ('batch_outbox', 'accounts', 'account_attributes', 'fee_requests')
				""", Integer.class));
		assertEquals(2, jdbcTemplate.queryForObject("""
				select count(*) from information_schema.columns where table_name = 'batch_outbox'
				""", Integer.class));
		assertEquals(3, jdbcTemplate.queryForObject("""
				select count(*) from information_schema.columns
				where table_name in ('accounts', 'account_attributes', 'fee_requests')
				and column_name = 'id' and data_type = 'bigint' and is_identity = 'YES'
				""", Integer.class));
		assertEquals(List.of("id"), jdbcTemplate.queryForList("""
				select attribute.attname from pg_constraint constraint_row
				join pg_attribute attribute on attribute.attrelid = constraint_row.conrelid
				and attribute.attnum = any(constraint_row.conkey)
				where constraint_row.conrelid = 'fee_requests'::regclass
				and constraint_row.contype = 'p'
				""", String.class));
	}

	@Test
	void rollsBackEveryGroupWhenPersistenceFailsPartWayThrough() throws Exception {
		jdbcTemplate.execute("""
				create function reject_fee_write() returns trigger language plpgsql as $$
				begin
				    raise exception 'Account Store write unavailable' using errcode = '08006';
				end;
				$$
				""");
		jdbcTemplate.execute("""
				create trigger reject_fee_write before insert on fee_requests
				for each row when (new.fee_request_id = 8) execute function reject_fee_write()
				""");
		try {
			mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
						.content(submission(UUID.randomUUID())))
					.andExpect(status().isServiceUnavailable())
					.andExpect(jsonPath("$.code").value("ACCOUNT_STORE_UNAVAILABLE"));
			assertRowCounts(0, 0, 0, 0);
		} finally {
			jdbcTemplate.execute("drop trigger reject_fee_write on fee_requests");
			jdbcTemplate.execute("drop function reject_fee_write()");
		}
	}

	@Test
	void returnsServiceUnavailableWhenTheAccountStoreCannotBeConnectedTo() throws Exception {
		try (Connection admin = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
				POSTGRES.getUsername(), POSTGRES.getPassword())) {
			try {
				admin.createStatement().execute("alter role account_app nologin");
				dataSource.getHikariPoolMXBean().softEvictConnections();
				mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
							.content(submission(UUID.randomUUID())))
						.andExpect(status().isServiceUnavailable())
						.andExpect(jsonPath("$.code").value("ACCOUNT_STORE_UNAVAILABLE"))
						.andExpect(jsonPath("$.message").value(
								"The Account Store is unavailable. Please try again later."));
			} finally {
				admin.createStatement().execute("alter role account_app login");
			}
		}
		assertRowCounts(0, 0, 0, 0);
	}

	@Test
	void describesLateFeeIdentityViolationsAndRollsBackTheBatch() throws Exception {
		jdbcTemplate.execute("""
				create function repeat_fee_identity() returns trigger language plpgsql as $$
				begin
				    new.fee_request_id := 7;
				    return new;
				end;
				$$
				""");
		jdbcTemplate.execute("""
				create trigger repeat_fee_identity before insert on fee_requests
				for each row execute function repeat_fee_identity()
				""");
		try {
			mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
					.content(submission(UUID.randomUUID())))
					.andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.code").value("INVALID_BATCH"))
					.andExpect(jsonPath("$.message", containsString(
							"Fee Request IDs must be unique within an account")))
					.andExpect(jsonPath("$.message", containsString("fee_requests")))
					.andExpect(jsonPath("$.message", containsString(
							"Key (account_id, fee_request_id)")));
			assertRowCounts(0, 0, 0, 0);
		} finally {
			jdbcTemplate.execute("drop trigger repeat_fee_identity on fee_requests");
			jdbcTemplate.execute("drop function repeat_fee_identity()");
		}
	}

	@Test
	void identifiesTheFailedStoredRecordForMonetaryConstraints() throws Exception {
		jdbcTemplate.execute("""
				create function invalidate_amount() returns trigger language plpgsql as $$
				begin
				    new.transaction_amount := 12.341;
				    return new;
				end;
				$$
				""");
		jdbcTemplate.execute("""
				create trigger invalidate_amount before insert on fee_requests
				for each row execute function invalidate_amount()
				""");
		try {
			mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
					.content(submission(UUID.randomUUID())))
					.andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.code").value("INVALID_BATCH"))
					.andExpect(jsonPath("$.message", containsString("fee_requests")))
					.andExpect(jsonPath("$.message", containsString("FEE001")))
					.andExpect(jsonPath("$.message", containsString("12.341")));
			assertRowCounts(0, 0, 0, 0);
		} finally {
			jdbcTemplate.execute("drop trigger invalidate_amount on fee_requests");
			jdbcTemplate.execute("drop function invalidate_amount()");
		}
	}

	@Test
	void rejectsMissingRequiredAttributesBeforeStoringAnyRows() throws Exception {
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(UUID.randomUUID()).replace("""
				    "attributes": [
				      {"code": "BALANCE", "value": 12345678901234567890.123456789},
				      {"code": "BALANCE", "value": "00123.40"},
				      {"code": "ACTIVE", "value": true}
				    ],
				""", "")))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_BATCH"));
		assertRowCounts(0, 0, 0, 0);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("recordErrors")
	void identifiesTheInvalidRecordInTheMessage(
			String scenario, String request, List<String> details) throws Exception {
		var response = mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content(request))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_BATCH"));
		for (String detail : details) {
			response.andExpect(jsonPath("$.message", containsString(detail)));
		}
		assertRowCounts(0, 0, 0, 0);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("invalidRequests")
	void rejectsInvalidSubmissionsWithoutStoringAnyRows(
			String scenario, String request, String message) throws Exception {
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content(request))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_BATCH"))
				.andExpect(jsonPath("$.message", not(emptyOrNullString())))
				.andExpect(jsonPath("$.message", containsString(message)));
		assertRowCounts(0, 0, 0, 0);
	}

	@ParameterizedTest
	@ValueSource(strings = {"0", "0.01", "12.3400", "12345678901234567890.12"})
	void preservesAcceptedMonetaryValuesAndOmittedAmounts(String amount) throws Exception {
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(UUID.randomUUID()).replace(
						"\"transactionAmount\": 12.34", "\"transactionAmount\": " + amount)))
				.andExpect(status().isAccepted());
		assertEquals(0, new BigDecimal(amount).compareTo(jdbcTemplate.queryForObject(
				"select transaction_amount from fee_requests where fee_request_id = 7",
				BigDecimal.class)));
		assertNull(jdbcTemplate.queryForObject(
				"select transaction_amount from fee_requests where fee_request_id = 8",
				BigDecimal.class));
		assertRowCounts(1, 1, 3, 2);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("attributeScalars")
	void preservesAttributeScalarMeaningInJpaAndJsonb(
			String json, Object expected, String jsonType) throws Exception {
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(UUID.randomUUID()).replace(
						"\"value\": true", "\"value\": " + json)))
				.andExpect(status().isAccepted());
		Object stored = entityManager.createQuery("""
				select attribute from AccountAttributeEntity attribute where attribute.code = 'ACTIVE'
				""", AccountAttributeEntity.class).getSingleResult().getValue().getValue();
		assertInstanceOf(expected.getClass(), stored);
		assertEquals(jsonType, jdbcTemplate.queryForObject(
				"select jsonb_typeof(value) from account_attributes where code = 'ACTIVE'",
				String.class));
		String text = jdbcTemplate.queryForObject(
				"select value #>> '{}' from account_attributes where code = 'ACTIVE'", String.class);
		if (expected instanceof BigDecimal number) {
			assertEquals(0, number.compareTo((BigDecimal) stored));
			assertEquals(0, number.compareTo(new BigDecimal(text)));
		} else {
			assertEquals(expected, stored);
			assertEquals(expected.toString(), text);
		}
		assertRowCounts(1, 1, 3, 2);
	}

	@Test
	void leavesRequiredAttributeAndConfiguredTypeChecksToEvaluation() throws Exception {
		String request = submission(UUID.randomUUID()).replace("FEE002", "FEE001");
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content(request))
				.andExpect(status().isAccepted());
		assertEquals(2, jdbcTemplate.queryForObject(
				"select count(*) from account_attributes where code = 'BALANCE'", Integer.class));
		assertEquals(2, jdbcTemplate.queryForObject(
				"select count(*) from fee_requests where code = 'FEE001'", Integer.class));
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(UUID.randomUUID()).replace("""
				    "attributes": [
				      {"code": "BALANCE", "value": 12345678901234567890.123456789},
				      {"code": "BALANCE", "value": "00123.40"},
				      {"code": "ACTIVE", "value": true}
				    ],
				""", "    \"attributes\": [],\n")))
				.andExpect(status().isAccepted());
		assertRowCounts(2, 2, 3, 4);
	}

	@ParameterizedTest
	@CsvSource({
			"batch_outbox, batch_id", "batch_outbox, processed",
			"accounts, batch_id", "accounts, account_number", "accounts, product_code",
			"accounts, branch_code", "accounts, pricing_date",
			"account_attributes, account_id", "account_attributes, code",
			"account_attributes, value", "fee_requests, account_id",
			"fee_requests, fee_request_id", "fee_requests, code"
	})
	void enforcesRequiredStoredValuesInPostgres(String table, String column) throws Exception {
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(UUID.randomUUID())))
				.andExpect(status().isAccepted());
		DataIntegrityViolationException failure = assertThrows(DataIntegrityViolationException.class,
				() -> jdbcTemplate.update("update " + table + " set " + column + " = null"));
		assertEquals("23502", assertInstanceOf(SQLException.class,
				failure.getMostSpecificCause()).getSQLState());
		assertRowCounts(1, 1, 3, 2);
	}

	@Test
	void enforcesScalarMoneyUniquenessAndForeignKeyConstraintsInPostgres() throws Exception {
		mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(UUID.randomUUID()))).andExpect(status().isAccepted());
		Long accountId = jdbcTemplate.queryForObject("select id from accounts", Long.class);
		for (String value : List.of("{}", "[]", "null")) {
			assertThrows(DataIntegrityViolationException.class, () -> jdbcTemplate.update(
					"insert into account_attributes (account_id, code, value) values (?, 'ATTR', ?::jsonb)",
					accountId, value));
		}
		assertThrows(DataIntegrityViolationException.class, () -> jdbcTemplate.update(
				"insert into account_attributes (account_id, code, value) values (?, 'ATTR', null)",
				accountId));
		for (BigDecimal amount : List.of(new BigDecimal("-0.01"), new BigDecimal("0.001"))) {
			assertThrows(DataIntegrityViolationException.class, () -> jdbcTemplate.update("""
					insert into fee_requests (account_id, fee_request_id, code, transaction_amount)
					values (?, 9, 'FEE', ?)
					""", accountId, amount));
		}
		assertThrows(DataIntegrityViolationException.class, () -> jdbcTemplate.update("""
				insert into accounts (batch_id, account_number, product_code, branch_code, pricing_date)
				select batch_id, account_number, product_code, branch_code, pricing_date from accounts
				"""));
		assertThrows(DataIntegrityViolationException.class, () -> jdbcTemplate.update("""
				insert into fee_requests (account_id, fee_request_id, code) values (?, 7, 'FEE')
				""", accountId));
		assertThrows(DataIntegrityViolationException.class, () -> jdbcTemplate.update("""
				insert into accounts (batch_id, account_number, product_code, branch_code, pricing_date)
				values (?, 'ORPHAN', 'PROD', 'BRANCH', current_date)
				""", UUID.randomUUID()));
		assertThrows(DataIntegrityViolationException.class, () -> jdbcTemplate.update("""
				insert into account_attributes (account_id, code, value) values (-1, 'ATTR', 'true')
				"""));
		assertThrows(DataIntegrityViolationException.class, () -> jdbcTemplate.update("""
				insert into fee_requests (account_id, fee_request_id, code) values (-1, 7, 'FEE')
				"""));
		Long secondAccount = jdbcTemplate.queryForObject("""
				insert into accounts (batch_id, account_number, product_code, branch_code, pricing_date)
				select batch_id, 'ACC002', product_code, branch_code, pricing_date from accounts
				returning id
				""", Long.class);
		jdbcTemplate.update("""
				insert into fee_requests (account_id, fee_request_id, code, transaction_amount)
				values (?, 7, 'FEE', 0)
				""", secondAccount);
		assertRowCounts(1, 2, 3, 3);
	}

	private List<List<Map<String, Object>>> storedWork() {
		return List.of(
				jdbcTemplate.queryForList("select * from batch_outbox order by batch_id"),
				jdbcTemplate.queryForList("select * from accounts order by id"),
				jdbcTemplate.queryForList("select * from account_attributes order by id"),
				jdbcTemplate.queryForList("select * from fee_requests order by id"));
	}

	private void assertRowCounts(int batches, int accounts, int attributes, int fees) {
		assertEquals(batches, jdbcTemplate.queryForObject(
				"select count(*) from batch_outbox", Integer.class));
		assertEquals(accounts, jdbcTemplate.queryForObject(
				"select count(*) from accounts", Integer.class));
		assertEquals(attributes, jdbcTemplate.queryForObject(
				"select count(*) from account_attributes", Integer.class));
		assertEquals(fees, jdbcTemplate.queryForObject(
				"select count(*) from fee_requests", Integer.class));
	}

	private static Stream<Arguments> recordErrors() {
		String account = """
				{"accountNumber":"ACC001","productCode":"PROD","branchCode":"BRANCH",
				 "pricingDate":"2026-10-21",
				 "attributes":[{"code":"TEXT","value":"value"},{"code":"ACTIVE","value":true}],
				 "fees":[{"feeRequestId":7,"code":"FEE001","transactionAmount":12.34}]}
				""".strip();
		String second = account.replace("ACC001", "ACC002");
		String prefix = "{\"batchId\":\"" + UUID.randomUUID() + "\",\"accounts\":[" + account + ",";
		Stream.Builder<Arguments> requests = Stream.builder();
		requests.add(arguments("unparseable document", "{", List.of("line", "column")));
		requests.add(arguments("duplicate account", prefix + account + "]}",
				List.of("accounts[1].accountNumber", "ACC001", "accounts[0].accountNumber")));
		requests.add(arguments("duplicate Fee Request", prefix + second.replace(
				"\"transactionAmount\":12.34}", "\"transactionAmount\":12.34},"
						+ "{\"feeRequestId\":7,\"code\":\"FEE002\"}") + "]}",
				List.of("accounts[1].fees[1].feeRequestId", "ACC002", "feeRequestId=7",
						"FEE002", "accounts[1].fees[0].feeRequestId")));
		requests.add(arguments("fractional-cent amount", prefix + second.replace(
				"12.34", "12.341") + "]}", List.of("accounts[1].fees[0].transactionAmount",
						"ACC002", "feeRequestId=7", "FEE001")));
		for (String value : List.of("{}", "[]")) {
			requests.add(arguments("malformed attribute " + value, prefix + second.replace(
					"\"value\":true", "\"value\":" + value) + "]}",
					List.of("accounts[1].attributes[1].value")));
		}
		requests.add(arguments("explicit null amount", prefix + second.replace(
				"12.34", "null") + "]}", List.of("accounts[1].fees[0].transactionAmount")));
		requests.add(arguments("invalid account number", prefix + second.replace(
				"ACC002", "bad-account") + "]}", List.of("accounts[1].accountNumber")));
		requests.add(arguments("invalid Fee code", prefix + second.replace(
				"FEE001", "bad-fee") + "]}", List.of("accounts[1].fees[0].code")));
		requests.add(arguments("null attribute", prefix + second.replace(
				"{\"code\":\"ACTIVE\",\"value\":true}", "null") + "]}",
				List.of("accounts[1].attributes[1]", "ACC002")));
		requests.add(arguments("null Fee Request", prefix + second.replace(
				"\"transactionAmount\":12.34}", "\"transactionAmount\":12.34},null") + "]}",
				List.of("accounts[1].fees[1]", "ACC002")));
		requests.add(arguments("null account", prefix + "null]}", List.of("accounts[1]")));
		return requests.build();
	}

	private static Stream<Arguments> invalidRequests() {
		UUID batchId = UUID.randomUUID();
		String valid = submission(batchId);
		Stream.Builder<Arguments> requests = Stream.builder();
		requests.add(arguments("malformed JSON", "{", "Request body is malformed"));
		requests.add(arguments("null body", "null", "Request body is malformed"));
		requests.add(arguments("missing batch ID", valid.replace(
				"\"batchId\": \"" + batchId + "\",", ""), "batchId"));
		requests.add(arguments("invalid batch ID", valid.replace(
				batchId.toString(), "not-a-uuid"), "Request body is malformed"));
		requests.add(arguments("missing accounts", "{\"batchId\":\"" + batchId + "\"}", "accounts"));
		for (String accounts : List.of("null", "[]", "[null]", "{}")) {
			requests.add(arguments("invalid accounts " + accounts,
					"{\"batchId\":\"" + batchId + "\",\"accounts\":" + accounts + "}",
					accounts.equals("{}") ? "Request body is malformed" : "ccounts"));
		}
		for (var field : Map.of("accountNumber", "ACC001", "productCode", "PROD001",
				"branchCode", "BR001", "pricingDate", "2026-10-21").entrySet()) {
			String property = "\"" + field.getKey() + "\": \"" + field.getValue() + "\"";
			requests.add(arguments("missing " + field.getKey(),
					valid.replace(property + ",", ""), field.getKey()));
			requests.add(arguments("null " + field.getKey(), valid.replace(
					property, "\"" + field.getKey() + "\": null"), field.getKey()));
		}
		for (var code : Map.of("ACC001", "accountNumber", "PROD001", "productCode",
				"BR001", "branchCode", "ACTIVE", "code", "FEE001", "code").entrySet()) {
			for (String invalid : List.of("", " ", "lowercase", "BAD-CODE", "A".repeat(26))) {
				requests.add(arguments("invalid " + code.getKey() + " " + invalid,
						valid.replace(code.getKey(), invalid), code.getValue()));
			}
		}
		requests.add(arguments("invalid date", valid.replace(
				"2026-10-21", "not-a-date"), "Request body is malformed"));
		requests.add(arguments("missing attribute code", valid.replace(
				"\"code\": \"ACTIVE\", ", ""), "code"));
		requests.add(arguments("missing attribute value", valid.replace(
				", \"value\": true", ""), "value"));
		requests.add(arguments("null attribute code", valid.replace(
				"\"code\": \"ACTIVE\"", "\"code\": null"), "code"));
		for (String value : List.of("{}", "[]", "null")) {
			requests.add(arguments("invalid attribute value " + value, valid.replace(
					"\"value\": true", "\"value\": " + value),
					value.equals("null") ? "value" : "Request body is malformed"));
		}
		requests.add(arguments("null attributes", valid.replace("""
				    "attributes": [
				      {"code": "BALANCE", "value": 12345678901234567890.123456789},
				      {"code": "BALANCE", "value": "00123.40"},
				      {"code": "ACTIVE", "value": true}
				    ],
				""", "    \"attributes\": null,\n"), "attributes"));
		requests.add(arguments("null attribute item", valid.replace(
				"{\"code\": \"ACTIVE\", \"value\": true}", "null"), "null items"));
		String fees = """
				    "fees": [{"feeRequestId": 7, "code": "FEE001", "transactionAmount": 12.34},
				             {"feeRequestId": 8, "code": "FEE002"}]
				""".strip();
		for (String value : List.of("null", "[]", "[null]")) {
			requests.add(arguments("invalid fees " + value, valid.replace(
					fees, "\"fees\": " + value), value.equals("[null]") ? "Fee Requests" : "fees"));
		}
		requests.add(arguments("missing fees", valid.replace(
				",\n    " + fees, ""), "fees"));
		requests.add(arguments("missing Fee Request ID", valid.replace(
				"\"feeRequestId\": 7, ", ""), "feeRequestId"));
		for (String id : List.of("null", "\"not-an-id\"", "9223372036854775808")) {
			requests.add(arguments("invalid Fee Request ID " + id, valid.replace(
					"\"feeRequestId\": 7", "\"feeRequestId\": " + id),
					id.equals("null") ? "feeRequestId" : "Request body is malformed"));
		}
		requests.add(arguments("duplicate Fee Request ID", valid.replace(
				"\"feeRequestId\": 8", "\"feeRequestId\": 7"),
				"Fee Request IDs must be unique within an account"));
		requests.add(arguments("missing Fee code", valid.replace(
				", \"code\": \"FEE002\"", ""), "code"));
		requests.add(arguments("null Fee code", valid.replace(
				"\"code\": \"FEE002\"", "\"code\": null"), "code"));
		for (String amount : List.of("null", "-0.01", "-0.001", "0.001", "0.009", "12.341")) {
			requests.add(arguments("invalid transaction amount " + amount, valid.replace(
					"\"transactionAmount\": 12.34", "\"transactionAmount\": " + amount),
					amount.equals("null") ? "Request body is malformed" : "mount"));
		}
		return requests.build();
	}

	private static Stream<Arguments> attributeScalars() {
		return Stream.of(
				arguments("\"00123.40\"", "00123.40", "string"),
				arguments("\"true\"", "true", "string"),
				arguments("\"false\"", "false", "string"),
				arguments("\"\"", "", "string"),
				arguments("true", Boolean.TRUE, "boolean"),
				arguments("false", Boolean.FALSE, "boolean"),
				arguments("9223372036854775808", new BigDecimal("9223372036854775808"), "number"),
				arguments("-12.3400", new BigDecimal("-12.34"), "number"),
				arguments("1e-9", new BigDecimal("0.000000001"), "number"),
				arguments("1e25", new BigDecimal("10000000000000000000000000"), "number"));
	}

	private static String submission(UUID batchId) {
		return """
				{
				  "batchId": "%s",
				  "accounts": [{
				    "accountNumber": "ACC001", "productCode": "PROD001",
				    "branchCode": "BR001", "pricingDate": "2026-10-21",
				    "attributes": [
				      {"code": "BALANCE", "value": 12345678901234567890.123456789},
				      {"code": "BALANCE", "value": "00123.40"},
				      {"code": "ACTIVE", "value": true}
				    ],
				    "fees": [{"feeRequestId": 7, "code": "FEE001", "transactionAmount": 12.34},
				             {"feeRequestId": 8, "code": "FEE002"}]
				  }]
				}
				""".formatted(batchId);
	}
}
