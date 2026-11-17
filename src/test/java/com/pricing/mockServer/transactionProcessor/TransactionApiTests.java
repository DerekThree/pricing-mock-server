package com.pricing.mockServer.transactionProcessor;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.networknt.schema.InputFormat;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TransactionApiTests {

	private static final String RECOMMENDATIONS = initializeRecommendations();
	private final JdbcTemplate recommendations = new JdbcTemplate(
			new DriverManagerDataSource(RECOMMENDATIONS, "sa", ""));
	private final HttpClient client = HttpClient.newHttpClient();
	private final JsonMapper json = JsonMapper.builder().build();

	@LocalServerPort
	private int port;

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
	}

	@BeforeEach
	void clearRecommendations() {
		recommendations.update("delete from recommendation_eligibility_reasons");
		recommendations.update("delete from fee_request_recommendations");
		recommendations.update("delete from account_recommendations");
		recommendations.update("delete from recommendation_outbox");
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
		WebSocket socket = client.newWebSocketBuilder()
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
