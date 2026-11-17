package com.pricing.mockServer.accountProcessor.accountStore;

import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class AccountRepository {

	private final EntityManager entityManager;

	public AccountRepository(EntityManager entityManager) {
		this.entityManager = entityManager;
	}

	@Transactional
	public void save(AccountOutboxEntity batch) {
		entityManager.persist(batch);
	}

	@Transactional(readOnly = true)
	public boolean existsByProcessedFalse() {
		return !entityManager.createQuery("""
				select 1 from AccountOutboxEntity batch where batch.processed = false
				""", Integer.class)
				.setMaxResults(1)
				.getResultList().isEmpty();
	}
}
