package com.pricing.mockServer.transactionProcessor.recommendationStore;

import com.pricing.mockServer.transactionProcessor.TransactionBatch;
import com.pricing.mockServer.transactionProcessor.TransactionBatch.AccountTransaction;
import com.pricing.mockServer.transactionProcessor.TransactionBatch.FeeTransaction;
import org.springframework.stereotype.Component;

@Component
public class RecommendationMapper {

	public TransactionBatch toTransactionBatch(RecommendationOutboxEntity batch) {
		return new TransactionBatch(batch.getBatchId(), batch.getAccounts().stream()
				.map(this::toAccountTransaction).toList());
	}

	private AccountTransaction toAccountTransaction(AccountRecommendationEntity account) {
		return new AccountTransaction(account.getAccountNumber(), account.getStatus(),
				account.getPricingPlanCode(),
				account.getStatus() == AccountRecommendationEntity.AccountStatus.OK
						? account.getFees().stream().map(this::toFeeTransaction).toList() : null);
	}

	private FeeTransaction toFeeTransaction(FeeRecommendationEntity fee) {
		return new FeeTransaction(fee.getFeeRequestId(), fee.getStatus(), fee.getAmount(),
				fee.getStatus() == FeeRecommendationEntity.FeeStatus.WAIVED
						? fee.getReasons() : null);
	}
}
