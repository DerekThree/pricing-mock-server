package com.pricing.mockserver.batch;

import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class BatchRepository {

	private final EntityManager entityManager;

	public BatchRepository(EntityManager entityManager) {
		this.entityManager = entityManager;
	}

	@Transactional
	public void save(BatchOutboxEntity batch) {
		entityManager.persist(batch);
	}

	@Transactional(readOnly = true)
	public boolean existsByProcessedFalse() {
		return !entityManager.createQuery("""
				select 1 from BatchOutboxEntity batch where batch.processed = false
				""", Integer.class)
				.setMaxResults(1)
				.getResultList().isEmpty();
	}
}
