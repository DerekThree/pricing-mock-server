package com.pricing.mockServer.transactionProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.pricing.mockServer.transactionProcessor.recommendationStore.RecommendationMapper;
import com.pricing.mockServer.transactionProcessor.recommendationStore.RecommendationRepository;

@Service
public class TransactionService {

	private static final Logger log = LoggerFactory.getLogger(TransactionService.class);

	private final RecommendationRepository recommendationRepository;
	private final RecommendationMapper mapper;
	private final TransactionWebSocketHandler transactionWebSocketHandler;

	public TransactionService(
			RecommendationRepository recommendationRepository, RecommendationMapper mapper,
			TransactionWebSocketHandler transactionWebSocketHandler) {
		this.recommendationRepository = recommendationRepository;
		this.mapper = mapper;
		this.transactionWebSocketHandler = transactionWebSocketHandler;
	}

	public void wakeUp() {
		Thread.startVirtualThread(this::forwardPendingTransactionBatch);
	}

	public void forwardPendingTransactionBatch() {
		try {
			recommendationRepository.findPending().ifPresent(batch -> {
				TransactionBatch result = mapper.toTransactionBatch(batch);
				try {
					transactionWebSocketHandler.send(result);
				} finally {
					recommendationRepository.markProcessed(result.batchId());
				}
			});
		} catch (RuntimeException ex) {
			log.error("Failed to forward pending Transaction Batch", ex);
		}
	}
}
