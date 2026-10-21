package com.pricing.mockserver.batch;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import com.pricing.mockserver.accountattribute.AccountAttributeEntity;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
				.andExpect(jsonPath("$.message").value(
						"Account numbers must be unique within the Pricing Batch"));
		assertRowCounts(2, 4, 0, 4);
	}

	@Test
	void publishesTheProviderOwnedContract() throws Exception {
		mockMvc.perform(get("/openapi.yaml"))
				.andExpect(status().isOk())
				.andExpect(content().string(org.hamcrest.Matchers.containsString(
						"title: Mock Account Processor API")));
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
	void retainsTheBackendSubmissionValidationBoundaries() throws Exception {
		String valid = submission(UUID.randomUUID());
		for (String invalid : List.of(
				valid.replace("\"value\": true", "\"value\": {}"),
				valid.replace("\"value\": true", "\"value\": []"),
				valid.replace("\"value\": true", "\"value\": null"),
				valid.replace("\"transactionAmount\": 12.34", "\"transactionAmount\": null"),
				valid.replace("\"transactionAmount\": 12.34", "\"transactionAmount\": -0.01"),
				valid.replace("\"transactionAmount\": 12.34", "\"transactionAmount\": 0.001"),
				valid.replace("\"feeRequestId\": 8", "\"feeRequestId\": 7"),
				valid.replace("\"accountNumber\": \"ACC001\"", "\"accountNumber\": \"lowercase\""))) {
			mockMvc.perform(post("/api/v1/batch").contentType(MediaType.APPLICATION_JSON)
						.content(invalid))
					.andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.code").value("INVALID_BATCH"));
		}
		assertRowCounts(0, 0, 0, 0);
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

	private String submission(UUID batchId) {
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
