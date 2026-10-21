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
}
