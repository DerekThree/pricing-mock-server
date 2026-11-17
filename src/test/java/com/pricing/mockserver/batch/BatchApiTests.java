package com.pricing.mockserver.batch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
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
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.awaitility.Awaitility.await;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class BatchApiTests {

	private static final String DATABASE_URL =
			"jdbc:h2:mem:batch-api;DATABASE_TO_LOWER=TRUE;NON_KEYWORDS=VALUE"
					+ ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";

	private static final HttpServer PRICING_ENGINE = startPricingEngine();

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private HikariDataSource dataSource;

	@DynamicPropertySource
	static void configureProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", () -> DATABASE_URL);
		registry.add("spring.datasource.username", () -> "sa");
		registry.add("spring.datasource.password", () -> "");
		registry.add("spring.datasource.driver-class-name", () -> "org.h2.Driver");
		registry.add("spring.flyway.enabled", () -> "false");
		registry.add("spring.sql.init.mode", () -> "always");
		registry.add("spring.sql.init.schema-locations", () -> "classpath:db/account-store-h2.sql");
		registry.add("spring.datasource.hikari.connection-timeout", () -> "500");
		registry.add("spring.datasource.hikari.validation-timeout", () -> "250");
		registry.add("spring.datasource.hikari.maximum-pool-size", () -> "2");
		registry.add("spring.flyway.locations", () -> "classpath:db/migration");
		registry.add("pricing-engine.wake-up-url", () ->
				"http://localhost:" + PRICING_ENGINE.getAddress().getPort() + "/work-available");
	}

	@AfterAll
	static void stopPricingEngine() {
		PRICING_ENGINE.stop(0);
	}

	private static HttpServer startPricingEngine() {
		try {
			HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
			server.createContext("/work-available", exchange -> {
				try (exchange) {
					exchange.sendResponseHeaders(204, -1);
				}
			});
			server.start();
			return server;
		} catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

	@AfterEach
	void cleanUp() {
		jdbcTemplate.update("delete from fee_requests");
		jdbcTemplate.update("delete from account_attributes");
		jdbcTemplate.update("delete from accounts");
		jdbcTemplate.update("delete from batch_outbox");
	}

	@ParameterizedTest
	@ValueSource(strings = { "http://localhost:5173", "http://127.0.0.1:5174" })
	void permitsLocalBrowserSubmissions(String origin) throws Exception {
		mockMvc.perform(options("/api/v1/mock/acc/batch")
				.header("Origin", origin)
				.header("Access-Control-Request-Method", "POST")
				.header("Access-Control-Request-Headers", "content-type"))
				.andExpect(status().isOk())
				.andExpect(header().string("Access-Control-Allow-Origin", origin))
				.andExpect(header().string("Access-Control-Allow-Credentials", "true"))
				.andExpect(header().string("Access-Control-Allow-Methods", containsString("POST")))
				.andExpect(header().string("Access-Control-Allow-Headers", "content-type"));
	}

	@Test
	void rejectsOtherBrowserOrigins() throws Exception {
		mockMvc.perform(options("/api/v1/mock/acc/batch")
				.header("Origin", "https://example.com")
				.header("Access-Control-Request-Method", "POST"))
				.andExpect(status().isForbidden())
				.andExpect(header().doesNotExist("Access-Control-Allow-Origin"));
	}

	@Nested
	@SpringBootTest(properties = "BATCH_MAX_ACCOUNTS=5000")
	class ConfiguredCapacity {

		@Autowired
		private MockMvc configuredMockMvc;

		@Test
		void acceptsTheConfiguredLimitAndRejectsOneMoreWithoutStoringIt() throws Exception {
			UUID batchId = UUID.randomUUID();
			configuredMockMvc.perform(post("/api/v1/mock/acc/batch")
						.contentType(MediaType.APPLICATION_JSON).content(submission(batchId, 5000)))
					.andExpect(status().isAccepted())
					.andExpect(jsonPath("$.batchId").value(batchId.toString()));
			assertRowCounts(1, 5000, 15000, 10000);
			assertEquals(batchId, jdbcTemplate.queryForObject(
					"select batch_id from batch_outbox", UUID.class));
			assertFalse(jdbcTemplate.queryForObject(
					"select processed from batch_outbox", Boolean.class));
			assertEquals(5000, jdbcTemplate.queryForObject("""
					select count(*) from accounts
					where batch_id = ? and product_code = 'PROD001' and branch_code = 'BR001'
					  and pricing_date = date '2026-10-21'
					""", Integer.class, batchId));
			assertEquals(5000, jdbcTemplate.queryForObject("""
					select count(*) from (
					    select account_id from account_attributes
					    group by account_id having count(*) = 3
					) complete_accounts
					""", Integer.class));
			assertEquals(5000, jdbcTemplate.queryForObject("""
					select count(*) from (
					    select account_id from fee_requests
					    group by account_id having count(*) = 2
					) complete_accounts
					""", Integer.class));

			configuredMockMvc.perform(post("/api/v1/mock/acc/batch")
						.contentType(MediaType.APPLICATION_JSON)
						.content(submission(UUID.randomUUID(), 5001)))
					.andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.code").value("INVALID_BATCH"))
					.andExpect(jsonPath("$.message").value(
							"Pricing Batch exceeds the limit of 5,000 accounts (5,001 submitted)."));
			assertRowCounts(1, 5000, 15000, 10000);
		}

		@Test
		void rollsBackTheConfiguredBatchWhenTheLastAccountCannotBeStored() throws Exception {
			jdbcTemplate.execute("""
					create trigger reject_last_account_write before insert on accounts for each row as
					'org.h2.api.Trigger create() {
					    return (connection, oldRow, newRow) -> {
					        if ("ACCOUNT4999".equals(newRow[2])) {
					            throw new java.sql.SQLException("Account Store write unavailable", "08006");
					        }
					    };
					}'
					""");
			try {
				configuredMockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
							.content(submission(UUID.randomUUID(), 5000)))
						.andExpect(status().isServiceUnavailable())
						.andExpect(jsonPath("$.code").value("ACCOUNT_STORE_UNAVAILABLE"));
				assertRowCounts(0, 0, 0, 0);
			} finally {
				jdbcTemplate.execute("drop trigger reject_last_account_write");
			}
		}
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
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
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
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
				.content("{\"batchId\":\"" + UUID.randomUUID() + "\"," + accounts + "}"))
				.andExpect(status().isAccepted());
		assertRowCounts(2, 4, 0, 4);
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
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
				.andExpect(content().string(containsString("/api/v1/mock/acc/batch:")))
				.andExpect(content().string(containsString("'409':")))
				.andExpect(content().string(containsString("BATCH_MAX_ACCOUNTS")))
				.andExpect(content().string(containsString("BATCH_ALREADY_ACCEPTED")));
	}

	@Test
	void rollsBackEveryGroupWhenPersistenceFailsPartWayThrough() throws Exception {
		jdbcTemplate.execute("""
				create trigger reject_fee_write before insert on fee_requests for each row as
				'org.h2.api.Trigger create() {
				    return (connection, oldRow, newRow) -> {
				        if (Long.valueOf(8).equals(newRow[2])) {
				            throw new java.sql.SQLException("Account Store write unavailable", "08006");
				        }
				    };
				}'
				""");
		try {
			mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
						.content(submission(UUID.randomUUID())))
					.andExpect(status().isServiceUnavailable())
					.andExpect(jsonPath("$.code").value("ACCOUNT_STORE_UNAVAILABLE"));
			assertRowCounts(0, 0, 0, 0);
		} finally {
			jdbcTemplate.execute("drop trigger reject_fee_write");
		}
	}

	@Test
	void returnsServiceUnavailableWhenTheAccountStoreCannotBeConnectedTo() throws Exception {
		String password = dataSource.getPassword();
		try {
			dataSource.setPassword("incorrect");
			dataSource.getHikariPoolMXBean().softEvictConnections();
			mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
					.content(submission(UUID.randomUUID())))
					.andExpect(status().isServiceUnavailable())
					.andExpect(jsonPath("$.code").value("ACCOUNT_STORE_UNAVAILABLE"))
					.andExpect(jsonPath("$.message").value(
							"The Account Store is unavailable. Please try again later."));
		} finally {
			dataSource.setPassword(password);
			dataSource.getHikariPoolMXBean().softEvictConnections();
		}
		await().atMost(Duration.ofSeconds(10))
				.ignoreException(CannotGetJdbcConnectionException.class)
				.untilAsserted(() -> assertRowCounts(0, 0, 0, 0));
	}

	@Test
	void rejectsMissingRequiredAttributesBeforeStoringAnyRows() throws Exception {
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
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
		var response = mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
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
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
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
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
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

	@Test
	void leavesRequiredAttributeAndConfiguredTypeChecksToEvaluation() throws Exception {
		String request = submission(UUID.randomUUID()).replace("FEE002", "FEE001");
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
				.content(request))
				.andExpect(status().isAccepted());
		assertEquals(2, jdbcTemplate.queryForObject(
				"select count(*) from account_attributes where code = 'BALANCE'", Integer.class));
		assertEquals(2, jdbcTemplate.queryForObject(
				"select count(*) from fee_requests where code = 'FEE001'", Integer.class));
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
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
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(UUID.randomUUID())))
				.andExpect(status().isAccepted());
		DataIntegrityViolationException failure = assertThrows(DataIntegrityViolationException.class,
				() -> jdbcTemplate.update("update " + table + " set " + column + " = null"));
		assertEquals("23502", assertInstanceOf(SQLException.class,
				failure.getMostSpecificCause()).getSQLState());
		assertRowCounts(1, 1, 3, 2);
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

	private static String submission(UUID batchId) {
		return submission(batchId, 1);
	}

	private static String submission(UUID batchId, int accountCount) {
		String account = """
				{
				    "accountNumber": "ACC001", "productCode": "PROD001",
				    "branchCode": "BR001", "pricingDate": "2026-10-21",
				    "attributes": [
				      {"code": "BALANCE", "value": 12345678901234567890.123456789},
				      {"code": "BALANCE", "value": "00123.40"},
				      {"code": "ACTIVE", "value": true}
				    ],
				    "fees": [{"feeRequestId": 7, "code": "FEE001", "transactionAmount": 12.34},
				             {"feeRequestId": 8, "code": "FEE002"}]
				  }
				""";
		String accounts = IntStream.range(0, accountCount)
				.mapToObj(index -> account.replace("ACC001",
						index == 0 ? "ACC001" : "ACCOUNT" + index))
				.collect(Collectors.joining(","));
		return "{\"batchId\": \"" + batchId + "\", \"accounts\": [" + accounts + "]}";
	}
}
