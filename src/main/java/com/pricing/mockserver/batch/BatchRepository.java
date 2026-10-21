package com.pricing.mockserver.batch;

import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Repository;

@Repository
public class BatchRepository {

	private final EntityManager entityManager;

	public BatchRepository(EntityManager entityManager) {
		this.entityManager = entityManager;
	}

	public void save(BatchOutboxEntity batch) {
		entityManager.persist(batch);
	}
}
