package com.pricing.mockServer.transactionProcessor.recommendationStore;

import java.util.Optional;
import java.util.UUID;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class RecommendationRepository {

	@PersistenceContext(unitName = "recommendations")
	private EntityManager entityManager;

	@Transactional(transactionManager = "recommendationTransactionManager", readOnly = true)
	public Optional<RecommendationOutboxEntity> findPending() {
		return entityManager.createQuery("""
				select batch from RecommendationOutboxEntity batch where batch.processed = false
				""", RecommendationOutboxEntity.class).setMaxResults(1).getResultList().stream()
				.findFirst().map(batch -> {
					entityManager.createQuery("""
							select distinct batch from RecommendationOutboxEntity batch
							left join fetch batch.accounts where batch.batchId = :batchId
							""", RecommendationOutboxEntity.class)
							.setParameter("batchId", batch.getBatchId()).getResultList();
					entityManager.createQuery("""
							select distinct account from AccountRecommendationEntity account
							left join fetch account.fees where account.batch.batchId = :batchId
							""", AccountRecommendationEntity.class)
							.setParameter("batchId", batch.getBatchId()).getResultList();
					entityManager.createQuery("""
							select distinct fee from FeeRecommendationEntity fee
							left join fetch fee.reasons where fee.account.batch.batchId = :batchId
							""", FeeRecommendationEntity.class)
							.setParameter("batchId", batch.getBatchId()).getResultList();
					return batch;
				});
	}

	@Transactional(transactionManager = "recommendationTransactionManager")
	public void markProcessed(UUID batchId) {
		entityManager.find(RecommendationOutboxEntity.class, batchId).setProcessed(true);
	}
}
