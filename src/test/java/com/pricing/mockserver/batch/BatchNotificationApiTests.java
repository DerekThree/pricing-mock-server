package com.pricing.mockserver.batch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.pricing.mockserver.MockServerApplication;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
		"PRICING_ENGINE_RETRY_DELAY=50ms", "PRICING_ENGINE_HTTP_TIMEOUT=500ms"
})
@AutoConfigureMockMvc
@Testcontainers(disabledWithoutDocker = true)
class BatchNotificationApiTests {

	@Container
	private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
			.withDatabaseName("account_store")
			.withInitScript("db/account-store-test-role.sql");

	private static final ExecutorService HTTP_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
	private static final List<WakeUp> WAKE_UPS = new CopyOnWriteArrayList<>();
	private static final AtomicInteger ACTIVE_REQUESTS = new AtomicInteger();
	private static final HttpServer PRICING_ENGINE = startPricingEngine();
	private static volatile JdbcTemplate accountStore;
	private static volatile int[] responses = {204};
	private static volatile long responseDelayMillis;
	private static volatile CountDownLatch responseGate = new CountDownLatch(0);

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@DynamicPropertySource
	static void configureProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
		registry.add("spring.datasource.username", () -> "account_app");
		registry.add("spring.datasource.password", () -> "account_test");
		registry.add("pricing-engine.wake-up-url", () ->
				"http://localhost:" + PRICING_ENGINE.getAddress().getPort() + "/work-available");
	}

	@AfterAll
	static void stopPricingEngine() {
		PRICING_ENGINE.stop(0);
		HTTP_EXECUTOR.close();
	}

	@BeforeEach
	void resetPricingEngine() {
		accountStore = jdbcTemplate;
		responses = new int[] {204};
		responseDelayMillis = 0;
		responseGate = new CountDownLatch(0);
	}

	@AfterEach
	void cleanUp() {
		responseGate.countDown();
		await().atMost(Duration.ofSeconds(3)).until(() -> ACTIVE_REQUESTS.get() == 0);
		jdbcTemplate.update("delete from fee_requests");
		jdbcTemplate.update("delete from account_attributes");
		jdbcTemplate.update("delete from accounts");
		jdbcTemplate.update("delete from batch_outbox");
		WAKE_UPS.clear();
	}

	@Test
	void notifiesOnlyAfterCommitWithoutBusinessPayload() throws Exception {
		UUID batchId = UUID.randomUUID();
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(batchId)))
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.batchId").value(batchId.toString()));

		await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertEquals(1, WAKE_UPS.size()));
		WakeUp wakeUp = WAKE_UPS.getFirst();
		assertEquals("POST", wakeUp.method());
		assertEquals("/work-available", wakeUp.path());
		assertNull(wakeUp.query());
		assertEquals("", wakeUp.body());
		assertEquals(1, wakeUp.rows().get(0).size());
		assertEquals(batchId, wakeUp.rows().get(0).getFirst().get("batch_id"));
		assertEquals(false, wakeUp.rows().get(0).getFirst().get("processed"));
		assertEquals(1, wakeUp.rows().get(1).size());
		assertEquals(1, wakeUp.rows().get(2).size());
		assertEquals(1, wakeUp.rows().get(3).size());
		assertAttempts(1);
		assertEquals(wakeUp.rows(), storedWork(jdbcTemplate));
		assertFalse(jdbcTemplate.queryForObject(
				"select processed from batch_outbox", Boolean.class));
	}

	@Test
	void acceptsWhileTheConsumerHasNotResponded() throws Exception {
		responseGate = new CountDownLatch(1);
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(UUID.randomUUID())))
				.andExpect(status().isAccepted());
		await().atMost(Duration.ofSeconds(3)).until(() -> WAKE_UPS.size() == 1);
		assertEquals(1, responseGate.getCount());
		responseGate.countDown();
		assertAttempts(1);
	}

	@Test
	void stopsRetryingAfterATransientFailureSucceeds() throws Exception {
		responses = new int[] {503, 204};
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(UUID.randomUUID())))
				.andExpect(status().isAccepted());
		assertAttempts(2);
	}

	@ParameterizedTest
	@ValueSource(ints = {400, 503, 302})
	void stopsAfterThreeAttemptsWhenTheConsumerNeverAccepts(int response) throws Exception {
		responses = new int[] {response};
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(UUID.randomUUID())))
				.andExpect(status().isAccepted());
		assertAttempts(3);
	}

	@Test
	void doesNotAdvertiseValidationFailuresDuplicatesOrFailedCommits() throws Exception {
		UUID batchId = UUID.randomUUID();
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(batchId).replace("12.34", "-1")))
				.andExpect(status().isBadRequest());
		assertEquals(0, WAKE_UPS.size());
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(batchId))).andExpect(status().isAccepted());
		assertAttempts(1);
		jdbcTemplate.update("update batch_outbox set processed = true where batch_id = ?", batchId);
		List<List<Map<String, Object>>> accepted = storedWork(jdbcTemplate);
		mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
				.content(submission(batchId)))
				.andExpect(status().isConflict());

		jdbcTemplate.execute("""
				create function reject_batch_commit() returns trigger language plpgsql as $$
				begin
				    raise exception 'Account Store commit unavailable' using errcode = '08006';
				end;
				$$
				""");
		jdbcTemplate.execute("""
				create constraint trigger reject_batch_commit after insert on batch_outbox
				deferrable initially deferred for each row execute function reject_batch_commit()
				""");
		try {
			mockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
					.content(submission(UUID.randomUUID())))
					.andExpect(status().isServiceUnavailable())
					.andExpect(jsonPath("$.code").value("ACCOUNT_STORE_UNAVAILABLE"));
			await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(2))
					.untilAsserted(() -> assertEquals(1, WAKE_UPS.size()));
			assertEquals(accepted, storedWork(jdbcTemplate));
		} finally {
			jdbcTemplate.execute("drop trigger reject_batch_commit on batch_outbox");
			jdbcTemplate.execute("drop function reject_batch_commit()");
		}
	}

	@Test
	void recoversPendingWorkAfterThePreviousInstanceExhaustedDelivery() throws Exception {
		responses = new int[] {503};
		List<List<Map<String, Object>>> accepted;
		try (ConfigurableApplicationContext application = startApplication()) {
			acceptBatch(application);
			assertAttempts(3);
			accepted = storedWork(jdbcTemplate);
		}
		WAKE_UPS.clear();
		responses = new int[] {204};
		try (ConfigurableApplicationContext application = startApplication()) {
			assertAttempts(1);
			assertEquals(accepted, storedWork(jdbcTemplate));
			WakeUp wakeUp = WAKE_UPS.getFirst();
			assertEquals("POST", wakeUp.method());
			assertEquals("/work-available", wakeUp.path());
			assertNull(wakeUp.query());
			assertEquals("", wakeUp.body());
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	void doesNotRecoverAnEmptyStoreOrOnlyProcessedRows(boolean processedRows) throws Exception {
		List<List<Map<String, Object>>> accepted;
		try (ConfigurableApplicationContext application = startApplication()) {
			if (processedRows) {
				acceptBatch(application);
				assertAttempts(1);
				jdbcTemplate.update("update batch_outbox set processed = true");
			}
			accepted = storedWork(jdbcTemplate);
		}
		WAKE_UPS.clear();
		try (ConfigurableApplicationContext application = startApplication()) {
			await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(2))
					.untilAsserted(() -> assertEquals(0, WAKE_UPS.size()));
			assertEquals(accepted, storedWork(jdbcTemplate));
		}
	}

	@Test
	void stopsRecoveryAfterATransientFailureSucceeds() throws Exception {
		List<List<Map<String, Object>>> accepted;
		try (ConfigurableApplicationContext application = startApplication()) {
			acceptBatch(application);
			assertAttempts(1);
			accepted = storedWork(jdbcTemplate);
		}
		WAKE_UPS.clear();
		responses = new int[] {503, 204};
		try (ConfigurableApplicationContext application = startApplication()) {
			assertAttempts(2);
			assertEquals(accepted, storedWork(jdbcTemplate));
		}
	}

	@Test
	void boundsRecoveryFailureAndRediscoversWorkOnAnotherRestart() throws Exception {
		List<List<Map<String, Object>>> accepted;
		try (ConfigurableApplicationContext application = startApplication()) {
			acceptBatch(application);
			assertAttempts(1);
			accepted = storedWork(jdbcTemplate);
		}
		responses = new int[] {503};
		for (int restart = 0; restart < 2; restart++) {
			WAKE_UPS.clear();
			try (ConfigurableApplicationContext application = startApplication()) {
				assertAttempts(3);
				assertEquals(accepted, storedWork(jdbcTemplate));
			}
		}
	}

	@Test
	void recoveryUsesTheConfiguredAttemptLimitDelayAndTimeout() throws Exception {
		List<List<Map<String, Object>>> accepted;
		try (ConfigurableApplicationContext application = startApplication()) {
			acceptBatch(application);
			assertAttempts(1);
			accepted = storedWork(jdbcTemplate);
		}
		WAKE_UPS.clear();
		responseDelayMillis = 1500;
		try (ConfigurableApplicationContext application = startApplication(
				"--pricing-engine.max-attempts=2", "--pricing-engine.retry-delay=150ms",
				"--pricing-engine.http-timeout=75ms")) {
			assertAttempts(2);
			Duration interval = Duration.ofNanos(
					WAKE_UPS.get(1).receivedAt() - WAKE_UPS.get(0).receivedAt());
			assertTrue(interval.compareTo(Duration.ofMillis(200)) >= 0);
			assertTrue(interval.compareTo(Duration.ofMillis(600)) < 0);
			assertEquals(accepted, storedWork(jdbcTemplate));
		}
	}

	@Nested
	@SpringBootTest(properties = {
			"PRICING_ENGINE_MAX_ATTEMPTS=2", "PRICING_ENGINE_RETRY_DELAY=150ms",
			"PRICING_ENGINE_HTTP_TIMEOUT=75ms"
	})
	class ConfiguredDelivery {

		@Autowired
		private MockMvc configuredMockMvc;

		@Test
		void usesTheConfiguredAttemptLimitAndDelay() throws Exception {
			responses = new int[] {503};
			configuredMockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
					.content(submission(UUID.randomUUID())))
					.andExpect(status().isAccepted());
			assertAttempts(2);
			assertTrue(Duration.ofNanos(WAKE_UPS.get(1).receivedAt() - WAKE_UPS.get(0).receivedAt())
					.compareTo(Duration.ofMillis(150)) >= 0);
		}

		@Test
		void countsTimedOutRequestsAndKeepsAcceptedRowsUnchanged() throws Exception {
			responseDelayMillis = 1500;
			configuredMockMvc.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
					.content(submission(UUID.randomUUID())))
					.andExpect(status().isAccepted());
			assertAttempts(2);
			Duration interval = Duration.ofNanos(
					WAKE_UPS.get(1).receivedAt() - WAKE_UPS.get(0).receivedAt());
			assertTrue(interval.compareTo(Duration.ofMillis(200)) >= 0);
			assertTrue(interval.compareTo(Duration.ofMillis(600)) < 0);
		}
	}

	private ConfigurableApplicationContext startApplication(String... properties) {
		List<String> arguments = new ArrayList<>(List.of(
				"--server.port=0",
				"--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
				"--spring.datasource.username=account_app",
				"--spring.datasource.password=account_test",
				"--pricing-engine.wake-up-url=http://localhost:"
						+ PRICING_ENGINE.getAddress().getPort() + "/work-available",
				"--PRICING_ENGINE_RETRY_DELAY=50ms", "--PRICING_ENGINE_HTTP_TIMEOUT=500ms"));
		arguments.addAll(List.of(properties));
		return new SpringApplicationBuilder(MockServerApplication.class)
				.run(arguments.toArray(String[]::new));
	}

	private void acceptBatch(ConfigurableApplicationContext application) throws Exception {
		MockMvcBuilders.webAppContextSetup((WebApplicationContext) application).build()
				.perform(post("/api/v1/mock/acc/batch").contentType(MediaType.APPLICATION_JSON)
						.content(submission(UUID.randomUUID())))
				.andExpect(status().isAccepted());
	}

	private void assertAttempts(int count) {
		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(count, WAKE_UPS.size()));
		await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(2))
				.untilAsserted(() -> assertEquals(count, WAKE_UPS.size()));
		List<List<Map<String, Object>>> accepted = storedWork(jdbcTemplate);
		WAKE_UPS.forEach(wakeUp -> assertEquals(accepted, wakeUp.rows()));
		assertEquals(false, accepted.getFirst().getFirst().get("processed"));
	}

	private static HttpServer startPricingEngine() {
		try {
			HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
			server.setExecutor(HTTP_EXECUTOR);
			server.createContext("/work-available", exchange -> {
				ACTIVE_REQUESTS.incrementAndGet();
				try (exchange) {
					int attempt = WAKE_UPS.size();
					WAKE_UPS.add(new WakeUp(exchange.getRequestMethod(),
							exchange.getRequestURI().getPath(), exchange.getRequestURI().getQuery(),
							new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8),
							storedWork(accountStore), System.nanoTime()));
					try {
						responseGate.await(3, TimeUnit.SECONDS);
						Thread.sleep(responseDelayMillis);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
						return;
					}
					exchange.sendResponseHeaders(responses[Math.min(attempt, responses.length - 1)], -1);
				} finally {
					ACTIVE_REQUESTS.decrementAndGet();
				}
			});
			server.start();
			return server;
		} catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

	private static List<List<Map<String, Object>>> storedWork(JdbcTemplate database) {
		return List.of(
				database.queryForList("select * from batch_outbox order by batch_id"),
				database.queryForList("select * from accounts order by id"),
				database.queryForList("select * from account_attributes order by id"),
				database.queryForList("select * from fee_requests order by id"));
	}

	private static String submission(UUID batchId) {
		return """
				{"batchId":"%s","accounts":[{
				 "accountNumber":"ACC001","productCode":"PROD001","branchCode":"BR001",
				 "pricingDate":"2026-10-21","attributes":[{"code":"ACTIVE","value":true}],
				 "fees":[{"feeRequestId":7,"code":"FEE001","transactionAmount":12.34}]}]}
				""".formatted(batchId);
	}

	private record WakeUp(String method, String path, String query, String body,
			List<List<Map<String, Object>>> rows, long receivedAt) {
	}
}
