package com.pricing.mockServer.transactionProcessor;

import java.sql.SQLException;
import java.time.Duration;
import java.util.function.Supplier;

import org.hibernate.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.Assert;

import com.pricing.mockServer.transactionProcessor.recommendationStore.RecommendationMapper;
import com.pricing.mockServer.transactionProcessor.recommendationStore.RecommendationRepository;

@Service
public class TransactionService {

	private static final Logger log = LoggerFactory.getLogger(TransactionService.class);

	private final RecommendationRepository recommendationRepository;
	private final RecommendationMapper mapper;
	private final TransactionWebSocketHandler transactionWebSocketHandler;
	private final int databaseMaxAttempts;
	private final Duration databaseRetryDelay;
	private final Duration databaseRecoveryCheckInterval;

	public TransactionService(
			RecommendationRepository recommendationRepository, RecommendationMapper mapper,
			TransactionWebSocketHandler transactionWebSocketHandler,
			@Value("${database.max-attempts}") int databaseMaxAttempts,
			@Value("${database.retry-delay}") Duration databaseRetryDelay,
			@Value("${database.recovery-check-interval}") Duration databaseRecoveryCheckInterval) {
		Assert.isTrue(databaseMaxAttempts > 0, "Database attempt limit must be positive");
		Assert.isTrue(!databaseRetryDelay.isNegative(), "Database retry delay must not be negative");
		Assert.isTrue(databaseRecoveryCheckInterval.isPositive(),
				"Database recovery check interval must be positive");
		this.databaseMaxAttempts = databaseMaxAttempts;
		this.databaseRetryDelay = databaseRetryDelay;
		this.databaseRecoveryCheckInterval = databaseRecoveryCheckInterval;
		this.recommendationRepository = recommendationRepository;
		this.mapper = mapper;
		this.transactionWebSocketHandler = transactionWebSocketHandler;
	}

	@EventListener(ApplicationReadyEvent.class)
	public void wakeUp() {
		Thread.startVirtualThread(this::forwardPendingTransactionBatch);
	}

	public synchronized void forwardPendingTransactionBatch() {
		try {
			while (processNextBatch()) { }
		} catch (RuntimeException ex) {
			log.error("Failed to forward pending Transaction Batch", ex);
		}
	}

	private boolean processNextBatch() {
		var pending = retryDatabaseOperation(recommendationRepository::findPending);
		if (pending.isEmpty()) {
			return false;
		}
		var outboxEntity = pending.get();
		log.info("Pricing Batch received: {}", outboxEntity.getBatchId());
		TransactionBatch batch = mapper.toTransactionBatch(outboxEntity);
		try {
			transactionWebSocketHandler.send(batch);
		} finally {
			retryDatabaseOperation(() -> {
				recommendationRepository.markProcessed(batch.batchId());
				return null;
			});
		}
		return true;
	}

	private <T> T retryDatabaseOperation(Supplier<T> operation) {
		for (int attempt = 1; ; attempt++) {
			try {
				return operation.get();
			} catch (RuntimeException ex) {
				if (!isTemporaryDatabaseFailure(ex)) {
					throw ex;
				}
				if (attempt == databaseMaxAttempts) {
					log.warn("Database operation exhausted {} attempts. Waiting for recovery", attempt);
					waitForDatabaseRecovery(ex);
					attempt = 0;
				} else {
					pause(databaseRetryDelay, ex);
				}
			}
		}
	}

	private void waitForDatabaseRecovery(RuntimeException failure) {
		while (true) {
			pause(databaseRecoveryCheckInterval, failure);
			try {
				recommendationRepository.checkConnection();
				log.info("Recommendation Store connectivity restored");
				return;
			} catch (RuntimeException ex) {
				if (!isTemporaryDatabaseFailure(ex)) {
					throw ex;
				}
				failure = ex;
			}
		}
	}

	private void pause(Duration delay, RuntimeException failure) {
		try {
			Thread.sleep(delay);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw failure;
		}
	}

	private boolean isTemporaryDatabaseFailure(RuntimeException failure) {
		return failure instanceof TransientDataAccessException
				|| failure instanceof RecoverableDataAccessException
				|| failure instanceof DataAccessResourceFailureException
				|| failure instanceof CannotCreateTransactionException
				// Hikari evicts a broken connection before Hibernate can roll back.
				|| (failure instanceof JpaSystemException
						&& failure.getCause() instanceof TransactionException transaction
						&& transaction.getCause() instanceof SQLException sql
						&& "Connection is closed".equals(sql.getMessage()));
	}
}
