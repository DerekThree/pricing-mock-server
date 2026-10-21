package com.pricing.mockserver.batch;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.pricing.mockserver.generated.model.BatchAccount;
import com.pricing.mockserver.generated.model.BatchFeeRequest;
import com.pricing.mockserver.generated.model.BatchRequest;
import org.springframework.stereotype.Component;

@Component
class BatchValidator {

	void validate(BatchRequest request) {
		Set<String> accountNumbers = new HashSet<>();
		for (BatchAccount account : request.getAccounts()) {
			if (account == null) {
				throw new IllegalArgumentException("Accounts must not contain null items");
			}

			if (!accountNumbers.add(account.getAccountNumber())) {
				throw new IllegalArgumentException("Account numbers must be unique within the Pricing Batch");
			}

			if (account.getAttributes().stream().anyMatch(Objects::isNull)) {
				throw new IllegalArgumentException("Account Attributes must not contain null items");
			}

			validateFees(account);
		}
	}

	private void validateFees(BatchAccount account) {
		Set<Long> feeRequestIds = new HashSet<>();
		for (BatchFeeRequest fee : account.getFees()) {
			if (fee == null) {
				throw new IllegalArgumentException("Fee Requests must not contain null items");
			}

			if (!feeRequestIds.add(fee.getFeeRequestId())) {
				throw new IllegalArgumentException("Fee Request IDs must be unique within an account");
			}

			if (fee.getTransactionAmount() != null
					&& fee.getTransactionAmount().remainder(new BigDecimal("0.01")).signum() != 0) {
				throw new IllegalArgumentException("Transaction Amount must use increments of 0.01");
			}
		}
	}
}

