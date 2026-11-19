package com.pricing.mockServer.transactionProcessor;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.networknt.schema.InputFormat;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import com.pricing.mockServer.MockServerApplication;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.WebSocketSessionDecorator;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TransactionApiTests {

	private static final String RECOMMENDATIONS = initializeRecommendations();
	private final JdbcTemplate recommendations = new JdbcTemplate(
			new DriverManagerDataSource(RECOMMENDATIONS, "sa", ""));
	private final HttpClient client = HttpClient.newHttpClient();
	private final JsonMapper json = JsonMapper.builder().build();

	@LocalServerPort
	private int port;

	@MockitoSpyBean
	private TransactionWebSocketHandler transactionWebSocketHandler;

	@MockitoSpyBean(name = "recommendationDataSource")
	private HikariDataSource recommendationDataSource;

	@DynamicPropertySource
	static void configureProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", () ->
				"jdbc:h2:mem:txn-accounts;DATABASE_TO_LOWER=TRUE;NON_KEYWORDS=VALUE;DB_CLOSE_DELAY=-1");
		registry.add("spring.datasource.username", () -> "sa");
		registry.add("spring.datasource.password", () -> "");
		registry.add("spring.datasource.driver-class-name", () -> "org.h2.Driver");
		registry.add("spring.sql.init.mode", () -> "always");
		registry.add("spring.sql.init.schema-locations", () -> "classpath:db/account-store-h2.sql");
		registry.add("recommendation-store.datasource.url", () -> RECOMMENDATIONS);
		registry.add("recommendation-store.datasource.username", () -> "recommendationdb_txn_processor");
		registry.add("recommendation-store.datasource.password", () -> "transaction_test");
		registry.add("recommendation-store.datasource.driver-class-name", () -> "org.h2.Driver");
		registry.add("recommendation-store.jpa.ddl-auto", () -> "validate");
		registry.add("database.max-attempts", () -> "2");
		registry.add("database.retry-delay", () -> "40ms");
		registry.add("database.recovery-check-interval", () -> "1s");
	}

	@BeforeEach
	void clearRecommendations() {
		recommendations.update("delete from recommendation_eligibility_reasons");
		recommendations.update("delete from fee_request_recommendations");
		recommendations.update("delete from account_recommendations");
		recommendations.update("delete from recommendation_outbox");
	}

	@Test
	void resumesPendingReadAfterBoundedRetriesWithoutAnotherWakeUp() throws Exception {
		UUID batchId = insertPendingBatch(1);
		BlockingQueue<String> messages = new LinkedBlockingQueue<>();
		WebSocket socket = connect(messages);
		AtomicBoolean disconnected = new AtomicBoolean();
		CountDownLatch refusedConnections = new CountDownLatch(2);
		doAnswer(call -> {
			if (disconnected.get()) {
				refusedConnections.countDown();
				throw new SQLTransientConnectionException("Recommendation Store offline", "08006");
			}
			return call.callRealMethod();
		}).when(recommendationDataSource).getConnection();
		try {
			recommendations.execute("set query_statistics true");
			blockStoreOperation("before select on account_recommendations");
			assertThat(wakeUp().statusCode()).isEqualTo(202);
			awaitRetryExhaustion();
			disconnected.set(true);
			assertThat(refusedConnections.await(4, TimeUnit.SECONDS)).isTrue();
			assertThat(messages).isEmpty();
			assertThat(processed(batchId)).isFalse();
			recommendations.execute("drop trigger store_outage");
			disconnected.set(false);
			String result = messages.poll(4, TimeUnit.SECONDS);
			assertThat(result).isNotNull();
			assertThat(json.readTree(result).get("batchId").asString()).isEqualTo(batchId.toString());
			await().atMost(Duration.ofSeconds(3)).until(() -> processed(batchId));
			assertThat(recommendations.queryForObject("""
					select sum(execution_count) from information_schema.query_statistics
					where lower(sql_statement) = 'select 1'
					""", Long.class)).isPositive();
		} finally {
			disconnected.set(false);
			clearStoreOutage();
			recommendations.execute("set query_statistics false");
			socket.sendClose(WebSocket.NORMAL_CLOSURE, "complete").join();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void recoversOnlyCompletionAfterDeliveryAttemptOrSkip(boolean connected) throws Exception {
		UUID first = insertPendingBatch(1);
		BlockingQueue<String> messages = new LinkedBlockingQueue<>();
		WebSocket socket = connected ? connect(messages) : null;
		try {
			blockStoreOperation("before update on recommendation_outbox");
			assertThat(wakeUp().statusCode()).isEqualTo(202);
			if (connected) {
				String sent = messages.poll(3, TimeUnit.SECONDS);
				assertThat(sent).isNotNull();
				assertThat(json.readTree(sent).get("batchId").asString()).isEqualTo(first.toString());
			}
			awaitRetryExhaustion();
			if (!connected) {
				socket = connect(messages);
			}
			UUID later = insertPendingBatch(2);
			assertThat(wakeUp().statusCode()).isEqualTo(202);
			assertThat(wakeUp().statusCode()).isEqualTo(202);
			await().during(Duration.ofMillis(250)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
				assertThat(messages).isEmpty();
				assertThat(processed(first)).isFalse();
				assertThat(processed(later)).isFalse();
			});
			recommendations.execute("drop trigger store_outage");
			String sent = messages.poll(4, TimeUnit.SECONDS);
			assertThat(sent).isNotNull();
			assertThat(json.readTree(sent).get("batchId").asString()).isEqualTo(later.toString());
			assertThat(processed(first)).isTrue();
			await().atMost(Duration.ofSeconds(3)).until(() -> processed(later));
			assertThat(messages.poll(250, TimeUnit.MILLISECONDS)).isNull();
		} finally {
			clearStoreOutage();
			if (socket != null) {
				socket.sendClose(WebSocket.NORMAL_CLOSURE, "complete").join();
			}
		}
	}

	private void blockStoreOperation(String event) {
		recommendations.execute("create sequence failed_operations");
		recommendations.execute("""
				create trigger store_outage %s as
				'org.h2.api.Trigger create() {
				    return (connection, oldRow, newRow) -> {
				        try (var statement = connection.createStatement()) {
				            statement.execute("select next value for failed_operations");
				        }
				        throw new java.sql.SQLException("Recommendation Store unavailable", "08006");
				    };
				}'
				""".formatted(event));
	}

	private void awaitRetryExhaustion() {
		await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(4)).untilAsserted(() ->
				assertThat(recommendations.queryForObject("""
						select base_value from information_schema.sequences
						where sequence_name = 'FAILED_OPERATIONS'
						""", Long.class)).isEqualTo(3L));
	}

	private void clearStoreOutage() {
		recommendations.execute("drop trigger if exists store_outage");
		recommendations.execute("drop sequence if exists failed_operations");
	}

	@Test
	void acknowledgesBodylessWakeUpWithoutBusinessData() throws Exception {
		HttpResponse<String> response = wakeUp();
		assertThat(response.statusCode()).isEqualTo(202);
		assertThat(response.body()).isEmpty();
	}

	@Test
	void deliversOneCompleteSuccessfulResultAndCompletesOnlyItsOutbox() throws Exception {
		UUID batchId = UUID.randomUUID();
		recommendations.update("insert into recommendation_outbox values (?, false)", batchId);
		recommendations.update("""
				insert into account_recommendations
				(id, batch_id, account_number, status, pricing_plan_code)
				values (1, ?, '10000001', 'OK', 'STANDARD'), (2, ?, '10000002', 'OK', 'PREMIUM')
				""", batchId, batchId);
		recommendations.update("""
				insert into fee_request_recommendations (id, account_id, fee_request_id, status, amount)
				values (1, 1, 1, 'CHARGED', 12.50), (2, 2, 1, 'WAIVED', null)
				""");
		recommendations.update("insert into recommendation_eligibility_reasons values (2, 'LOYALTY')");
		List<List<Map<String, Object>>> stored = storedRecommendations();
		BlockingQueue<String> messages = new LinkedBlockingQueue<>();
		WebSocket socket = connect(messages);
		try {
			String message;
			try (var lock = DriverManager.getConnection(RECOMMENDATIONS, "sa", "")) {
				lock.setAutoCommit(false);
				try (var statement = lock.prepareStatement(
						"select * from recommendation_outbox where batch_id = ? for update")) {
					statement.setObject(1, batchId);
					statement.executeQuery().close();
				}
				assertThat(wakeUp().statusCode()).isEqualTo(202);
				message = messages.poll(3, TimeUnit.SECONDS);
				assertThat(message).isNotNull();
				assertThat(processed(batchId)).isFalse();
				lock.commit();
			}
			String schema = new String(getClass().getResourceAsStream(
					"/static/schemas/transaction-batch-v1.json").readAllBytes(), StandardCharsets.UTF_8);
			var validator = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_7)
					.getSchema(schema, InputFormat.JSON);
			assertThat(validator.validate(message, InputFormat.JSON)).isEmpty();
			assertThat(json.readTree(message)).isEqualTo(json.readTree("""
					{"batchId":"%s","accounts":[
					 {"accountNumber":"10000001","status":"OK","pricingPlanCode":"STANDARD",
					  "fees":[{"feeRequestId":1,"status":"CHARGED","amount":12.5,"reasons":null}]},
					 {"accountNumber":"10000002","status":"OK","pricingPlanCode":"PREMIUM",
					  "fees":[{"feeRequestId":1,"status":"WAIVED","amount":null,"reasons":["LOYALTY"]}]}
					]}
					""".formatted(batchId)));
			await().atMost(Duration.ofSeconds(3)).until(() -> processed(batchId));
			assertThat(storedRecommendations()).isEqualTo(stored);
			assertThat(recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class)).isEqualTo(1);
			assertThat(messages).isEmpty();
		} finally {
			socket.sendClose(WebSocket.NORMAL_CLOSURE, "complete").join();
		}
	}

	@Test
	void deliversEveryStoredOutcomeTogetherWithoutChangingRecommendations() throws Exception {
		UUID batchId = UUID.randomUUID();
		recommendations.update("insert into recommendation_outbox values (?, false)", batchId);
		recommendations.update("""
				insert into account_recommendations
				(id, batch_id, account_number, status, pricing_plan_code)
				values (10, ?, '10000009', 'OK', 'STANDARD'),
				       (20, ?, '10000001', 'OK', 'PREMIUM'),
				       (30, ?, '10000008', 'BRANCH_NOT_FOUND', null),
				       (40, ?, '10000002', 'REGION_NOT_FOUND', null),
				       (50, ?, '10000007', 'PLAN_NOT_FOUND', null),
				       (60, ?, '10000003', 'DUPLICATE_ATTRIBUTE', null),
				       (70, ?, '10000006', 'INVALID_ATTRIBUTE_TYPE', null),
				       (80, ?, '10000004', 'MISSING_ATTRIBUTE', null),
				       (90, ?, '10000005', 'ERROR', null)
				""", batchId, batchId, batchId, batchId, batchId, batchId, batchId, batchId, batchId);
		recommendations.update("""
				insert into fee_request_recommendations (id, account_id, fee_request_id, status, amount)
				values (10, 10, 9, 'CHARGED', 123456789.25), (20, 10, 2, 'WAIVED', null),
				       (30, 10, 8, 'FEE_NOT_FOUND', null), (40, 10, 3, 'MISSING_TRANSACTION', null),
				       (50, 10, 7, 'INVALID_TRANSACTION_AMOUNT', null),
				       (60, 10, 4, 'INVALID_ELIGIBILITY_CONDITION', null),
				       (70, 10, 6, 'ERROR', null), (80, 20, 9, 'CHARGED', 0),
				       (90, 20, 2, 'WAIVED', null)
				""");
		recommendations.update("""
				insert into recommendation_eligibility_reasons
				values (20, 'LOYALTY'), (20, 'BALANCE')
				""");
		List<List<Map<String, Object>>> stored = storedRecommendations();
		BlockingQueue<String> messages = new LinkedBlockingQueue<>();
		WebSocket socket = connect(messages);
		try {
			assertThat(wakeUp().statusCode()).isEqualTo(202);
			String message = messages.poll(3, TimeUnit.SECONDS);
			assertThat(message).isNotNull();
			String schema = new String(getClass().getResourceAsStream(
					"/static/schemas/transaction-batch-v1.json").readAllBytes(), StandardCharsets.UTF_8);
			var validator = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_7)
					.getSchema(schema, InputFormat.JSON);
			assertThat(validator.validate(message, InputFormat.JSON)).isEmpty();
			assertThat(json.readValue(message, TransactionBatch.class)).usingRecursiveComparison()
					.ignoringCollectionOrder().isEqualTo(json.readValue("""
					{"batchId":"%s","accounts":[
					 {"accountNumber":"10000001","status":"OK","pricingPlanCode":"PREMIUM",
					  "fees":[{"feeRequestId":2,"status":"WAIVED","amount":null,"reasons":[]},
					          {"feeRequestId":9,"status":"CHARGED","amount":0,"reasons":null}]},
					 {"accountNumber":"10000009","status":"OK","pricingPlanCode":"STANDARD",
					  "fees":[
					   {"feeRequestId":2,"status":"WAIVED","amount":null,"reasons":["BALANCE","LOYALTY"]},
					   {"feeRequestId":3,"status":"MISSING_TRANSACTION","amount":null,"reasons":null},
					   {"feeRequestId":4,"status":"INVALID_ELIGIBILITY_CONDITION","amount":null,"reasons":null},
					   {"feeRequestId":6,"status":"ERROR","amount":null,"reasons":null},
					   {"feeRequestId":7,"status":"INVALID_TRANSACTION_AMOUNT","amount":null,"reasons":null},
					   {"feeRequestId":8,"status":"FEE_NOT_FOUND","amount":null,"reasons":null},
					   {"feeRequestId":9,"status":"CHARGED","amount":123456789.25,"reasons":null}]},
					 {"accountNumber":"10000002","status":"REGION_NOT_FOUND","pricingPlanCode":null,
					  "fees":null},
					 {"accountNumber":"10000003","status":"DUPLICATE_ATTRIBUTE","pricingPlanCode":null,
					  "fees":null},
					 {"accountNumber":"10000004","status":"MISSING_ATTRIBUTE","pricingPlanCode":null,
					  "fees":null},
					 {"accountNumber":"10000005","status":"ERROR","pricingPlanCode":null,"fees":null},
					 {"accountNumber":"10000006","status":"INVALID_ATTRIBUTE_TYPE","pricingPlanCode":null,
					  "fees":null},
					 {"accountNumber":"10000007","status":"PLAN_NOT_FOUND","pricingPlanCode":null,
					  "fees":null},
					 {"accountNumber":"10000008","status":"BRANCH_NOT_FOUND","pricingPlanCode":null,
					  "fees":null}
					]}
					""".formatted(batchId), TransactionBatch.class));
			await().atMost(Duration.ofSeconds(3)).until(() -> processed(batchId));
			assertThat(storedRecommendations()).isEqualTo(stored);
			assertThat(messages).isEmpty();
		} finally {
			socket.sendClose(WebSocket.NORMAL_CLOSURE, "complete").join();
		}
	}

	@Test
	void discoversPendingBatchesOnStartupAndRestartWithoutAWakeUp() {
		UUID completed = UUID.randomUUID();
		recommendations.update("insert into recommendation_outbox values (?, true)", completed);
		List<UUID> batchIds = new ArrayList<>();
		for (int instance = 0; instance < 2; instance++) {
			UUID batchId = UUID.randomUUID();
			batchIds.add(batchId);
			recommendations.update("insert into recommendation_outbox values (?, false)", batchId);
			recommendations.update("""
					insert into account_recommendations
					(id, batch_id, account_number, status, pricing_plan_code)
					values (?, ?, '10000001', 'OK', 'STANDARD')
					""", instance + 1, batchId);
			List<List<Map<String, Object>>> stored = storedRecommendations();
			try (var application = startApplication()) {
				await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
					for (UUID pending : batchIds) {
						assertThat(processed(pending)).isTrue();
					}
				});
				assertThat(processed(completed)).isTrue();
				assertThat(storedRecommendations()).isEqualTo(stored);
				assertThat(recommendations.queryForObject(
						"select count(*) from recommendation_outbox", Integer.class))
						.isEqualTo(batchIds.size() + 1);
			}
		}
	}

	@Test
	void processesAllPendingBatchesAfterOneWakeUp() throws Exception {
		List<UUID> batchIds = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
		for (int i = 0; i < batchIds.size(); i++) {
			recommendations.update("insert into recommendation_outbox values (?, false)",
					batchIds.get(i));
			recommendations.update("""
					insert into account_recommendations
					(id, batch_id, account_number, status, pricing_plan_code)
					values (?, ?, ?, 'OK', 'STANDARD')
					""", i + 1, batchIds.get(i), "1000000" + i);
		}
		List<List<Map<String, Object>>> stored = storedRecommendations();
		BlockingQueue<String> messages = new LinkedBlockingQueue<>();
		WebSocket socket = connect(messages);
		try {
			assertThat(wakeUp().statusCode()).isEqualTo(202);
			List<UUID> received = new ArrayList<>();
			for (int i = 0; i < batchIds.size(); i++) {
				String message = messages.poll(3, TimeUnit.SECONDS);
				assertThat(message).as("pending batch %s", i + 1).isNotNull();
				var result = json.readTree(message);
				UUID batchId = UUID.fromString(result.get("batchId").asString());
				received.add(batchId);
				assertThat(result.get("accounts").get(0).get("accountNumber").asString())
						.isEqualTo("1000000" + batchIds.indexOf(batchId));
			}
			assertThat(received).containsExactlyInAnyOrderElementsOf(batchIds);
			await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
				for (UUID batchId : batchIds) {
					assertThat(processed(batchId)).isTrue();
				}
			});
			assertThat(storedRecommendations()).isEqualTo(stored);
			assertThat(messages).isEmpty();
		} finally {
			socket.sendClose(WebSocket.NORMAL_CLOSURE, "complete").join();
		}
	}

	@Test
	void overlappingAndDuplicateWakeUpsWaitForCompletionAndSkipProcessedBatches() throws Exception {
		UUID completed = UUID.randomUUID();
		recommendations.update("insert into recommendation_outbox values (?, true)", completed);
		List<UUID> batchIds = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
		for (int i = 0; i < batchIds.size(); i++) {
			recommendations.update("insert into recommendation_outbox values (?, false)",
					batchIds.get(i));
			recommendations.update("""
					insert into account_recommendations
					(id, batch_id, account_number, status, pricing_plan_code)
					values (?, ?, ?, 'OK', 'STANDARD')
					""", i + 1, batchIds.get(i), "1000000" + i);
		}
		List<List<Map<String, Object>>> stored = storedRecommendations();
		BlockingQueue<String> messages = new LinkedBlockingQueue<>();
		WebSocket socket = connect(messages);
		try {
			List<UUID> received = new ArrayList<>();
			try (var lock = DriverManager.getConnection(RECOMMENDATIONS, "sa", "")) {
				lock.setAutoCommit(false);
				try (var statement = lock.prepareStatement(
						"select * from recommendation_outbox where processed = false for update")) {
					statement.executeQuery().close();
				}
				assertThat(wakeUp().statusCode()).isEqualTo(202);
				String first = messages.poll(3, TimeUnit.SECONDS);
				assertThat(first).isNotNull();
				received.add(UUID.fromString(json.readTree(first).get("batchId").asString()));
				for (int duplicate = 0; duplicate < 4; duplicate++) {
					HttpResponse<String> response = wakeUp();
					assertThat(response.statusCode()).isEqualTo(202);
					assertThat(response.body()).isEmpty();
				}
				await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(1))
						.untilAsserted(() -> {
							assertThat(messages).isEmpty();
							for (UUID batchId : batchIds) {
								assertThat(processed(batchId)).isFalse();
							}
						});
				lock.commit();
			}
			for (int i = 1; i < batchIds.size(); i++) {
				String message = messages.poll(3, TimeUnit.SECONDS);
				assertThat(message).as("pending batch %s", i + 1).isNotNull();
				assertThat(processed(received.get(i - 1))).isTrue();
				received.add(UUID.fromString(json.readTree(message).get("batchId").asString()));
			}
			assertThat(received).containsExactlyInAnyOrderElementsOf(batchIds);
			await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
				for (UUID batchId : batchIds) {
					assertThat(processed(batchId)).isTrue();
				}
			});
			assertThat(wakeUp().statusCode()).isEqualTo(202);
			await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
					.untilAsserted(() -> assertThat(messages).isEmpty());
			assertThat(processed(completed)).isTrue();
			assertThat(storedRecommendations()).isEqualTo(stored);
			assertThat(recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class)).isEqualTo(4);
		} finally {
			socket.sendClose(WebSocket.NORMAL_CLOSURE, "complete").join();
		}
	}

	@Test
	void skipsDisconnectedDeliveryAndResumesOnlyFutureLiveDelivery() throws Exception {
		BlockingQueue<UUID> attempts = new LinkedBlockingQueue<>();
		beforeFirstSocketWrites(attempts::add);
		BlockingQueue<String> messages = new LinkedBlockingQueue<>();
		WebSocket disconnected = connect(messages);
		disconnected.sendClose(WebSocket.NORMAL_CLOSURE, "disconnect").join();
		await().atMost(Duration.ofSeconds(3)).until(disconnected::isInputClosed);

		UUID missed = insertPendingBatch(1);
		assertThat(wakeUp().statusCode()).isEqualTo(202);
		await().atMost(Duration.ofSeconds(3)).until(() -> processed(missed));
		assertThat(attempts).isEmpty();

		UUID future = insertPendingBatch(2);
		WebSocket reconnected = connect(messages);
		try {
			await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
				assertThat(messages).isEmpty();
				assertThat(processed(future)).isFalse();
			});
			assertThat(wakeUp().statusCode()).isEqualTo(202);
			String message = messages.poll(3, TimeUnit.SECONDS);
			assertThat(message).isNotNull();
			assertThat(json.readTree(message).get("batchId").asString()).isEqualTo(future.toString());
			await().atMost(Duration.ofSeconds(3)).until(() -> processed(future));
			assertThat(messages).isEmpty();
		} finally {
			reconnected.sendClose(WebSocket.NORMAL_CLOSURE, "complete").join();
		}
	}

	@Test
	void completesFailedSendAndContinuesTheDrainWithoutRetryOrReplay() throws Exception {
		BlockingQueue<UUID> attempts = new LinkedBlockingQueue<>();
		beforeFirstSocketWrites(batchId -> {
			boolean firstAttempt = attempts.isEmpty();
			attempts.add(batchId);
			if (firstAttempt) {
				throw new IllegalStateException("Socket closed during write");
			}
		});

		UUID first = insertPendingBatch(1);
		UUID second = insertPendingBatch(2);
		BlockingQueue<String> messages = new LinkedBlockingQueue<>();
		WebSocket socket = connect(messages);
		try {
			assertThat(wakeUp().statusCode()).isEqualTo(202);
			await().atMost(Duration.ofSeconds(3)).until(() -> processed(first) && processed(second));
			assertThat(attempts).containsExactlyInAnyOrder(first, second);
			String message = messages.poll(3, TimeUnit.SECONDS);
			assertThat(message).isNotNull();
			assertThat(json.readTree(message).get("batchId").asString())
					.isEqualTo(List.copyOf(attempts).getLast().toString());
		} finally {
			socket.sendClose(WebSocket.NORMAL_CLOSURE, "disconnect").join();
		}

		WebSocket reconnected = connect(messages);
		try {
			assertThat(wakeUp().statusCode()).isEqualTo(202);
			await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
					.untilAsserted(() -> assertThat(messages).isEmpty());
		} finally {
			reconnected.sendClose(WebSocket.NORMAL_CLOSURE, "complete").join();
		}
	}

	private void beforeFirstSocketWrites(Consumer<UUID> beforeWrite) {
		doAnswer(invocation -> {
			WebSocketSession session = invocation.getArgument(0);
			transactionWebSocketHandler.afterConnectionEstablished(new WebSocketSessionDecorator(session) {
				@Override
				public void sendMessage(WebSocketMessage<?> message) throws IOException {
					var batch = json.readTree((String) message.getPayload());
					beforeWrite.accept(UUID.fromString(batch.get("batchId").asString()));
					super.sendMessage(message);
				}
			});
			return null;
		}).doCallRealMethod().when(transactionWebSocketHandler).afterConnectionEstablished(any());
	}

	private UUID insertPendingBatch(int id) {
		UUID batchId = UUID.randomUUID();
		recommendations.update("insert into recommendation_outbox values (?, false)", batchId);
		recommendations.update("""
				insert into account_recommendations
				(id, batch_id, account_number, status, pricing_plan_code)
				values (?, ?, ?, 'OK', 'STANDARD')
				""", id, batchId, "1000000" + id);
		return batchId;
	}

	private ConfigurableApplicationContext startApplication() {
		return new SpringApplicationBuilder(MockServerApplication.class).run(
				"--server.port=0",
				"--spring.datasource.url=jdbc:h2:mem:txn-startup-accounts;DATABASE_TO_LOWER=TRUE;"
						+ "NON_KEYWORDS=VALUE;DB_CLOSE_DELAY=-1",
				"--spring.datasource.username=sa", "--spring.datasource.password=",
				"--spring.datasource.driver-class-name=org.h2.Driver",
				"--spring.flyway.enabled=false", "--spring.sql.init.mode=always",
				"--spring.sql.init.schema-locations=classpath:db/account-store-h2.sql",
				"--recommendation-store.datasource.url=" + RECOMMENDATIONS,
				"--recommendation-store.datasource.username=recommendationdb_txn_processor",
				"--recommendation-store.datasource.password=transaction_test",
				"--recommendation-store.datasource.driver-class-name=org.h2.Driver",
				"--recommendation-store.jpa.ddl-auto=validate");
	}

	private WebSocket connect(BlockingQueue<String> messages) {
		return client.newWebSocketBuilder()
				.buildAsync(URI.create("ws://localhost:" + port + "/api/v1/mock/txn/batches"),
						new WebSocket.Listener() {
							private final StringBuilder message = new StringBuilder();

							@Override
							public void onOpen(WebSocket connection) {
								connection.request(1);
							}

							@Override
							public CompletionStage<?> onText(
									WebSocket connection, CharSequence data, boolean last) {
								message.append(data);
								if (last) {
									messages.add(message.toString());
									message.setLength(0);
								}
								connection.request(1);
								return null;
							}
						}).join();
	}

	private HttpResponse<String> wakeUp() throws Exception {
		return client.send(HttpRequest.newBuilder(
				URI.create("http://localhost:" + port + "/api/v1/mock/txn/wake"))
				.timeout(Duration.ofSeconds(3)).POST(HttpRequest.BodyPublishers.noBody()).build(),
				HttpResponse.BodyHandlers.ofString());
	}

	private boolean processed(UUID batchId) {
		return recommendations.queryForObject(
				"select processed from recommendation_outbox where batch_id = ?", Boolean.class, batchId);
	}

	private List<List<Map<String, Object>>> storedRecommendations() {
		return List.of("account_recommendations", "fee_request_recommendations",
				"recommendation_eligibility_reasons").stream()
				.map(table -> recommendations.queryForList("select * from " + table)).toList();
	}

	private static String initializeRecommendations() {
		String url = "jdbc:h2:mem:txn-recommendations;MODE=PostgreSQL";
		Flyway.configure().dataSource(url + ";DB_CLOSE_DELAY=-1", "sa", "")
				.locations("classpath:db/recommendation")
				.load().migrate();
		JdbcTemplate admin = new JdbcTemplate(new DriverManagerDataSource(url, "sa", ""));
		admin.execute("create user recommendationdb_txn_processor password 'transaction_test'");
		admin.execute("""
				grant select on recommendation_outbox, account_recommendations,
				fee_request_recommendations, recommendation_eligibility_reasons
				to recommendationdb_txn_processor
				""");
		admin.execute("grant update on recommendation_outbox to recommendationdb_txn_processor");
		return url;
	}
}
