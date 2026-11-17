package com.pricing.mockServer.transactionProcessor;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import com.pricing.mockServer.transactionProcessor.recommendationStore.AccountRecommendationEntity;
import com.pricing.mockServer.transactionProcessor.recommendationStore.FeeRecommendationEntity;

public record TransactionBatch(UUID batchId, List<AccountTransaction> accounts) {

	public TransactionBatch {
		accounts = List.copyOf(accounts);
	}

	public record AccountTransaction(
			String accountNumber,
			AccountRecommendationEntity.AccountStatus status,
			String pricingPlanCode,
			List<FeeTransaction> fees) {

		public AccountTransaction {
			fees = fees == null ? null : List.copyOf(fees);
		}
	}

	public record FeeTransaction(
			Long feeRequestId,
			FeeRecommendationEntity.FeeStatus status,
			BigDecimal amount,
			List<String> reasons) {

		public FeeTransaction {
			reasons = reasons == null ? null : List.copyOf(reasons);
		}
	}
}
