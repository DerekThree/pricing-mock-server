package com.pricing.mockserver.batch;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.pricing.mockserver.generated.model.BatchAccount;
import com.pricing.mockserver.generated.model.BatchFeeRequest;
import com.pricing.mockserver.generated.model.BatchRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class BatchValidator {

	private final int maxAccounts;

	BatchValidator(@Value("${batch.max-accounts}") int maxAccounts) {
		this.maxAccounts = maxAccounts;
	}

	void validate(BatchRequest request) {
		if (request.getAccounts().size() > maxAccounts) {
			throw new IllegalArgumentException(String.format(Locale.ROOT,
					"Pricing Batch exceeds the limit of %,d accounts (%,d submitted).",
					maxAccounts, request.getAccounts().size()));
		}

		Map<String, Integer> accountNumbers = new HashMap<>();
		for (int index = 0; index < request.getAccounts().size(); index++) {
			BatchAccount account = request.getAccounts().get(index);
			String path = "accounts[" + index + "]";
			if (account == null) {
				throw new IllegalArgumentException("Accounts must not contain null items: " + path);
			}

			String context = " (accountNumber=" + account.getAccountNumber() + ")";
			Integer duplicate = accountNumbers.putIfAbsent(account.getAccountNumber(), index);
			if (duplicate != null) {
				throw new IllegalArgumentException(
						"Account numbers must be unique within the Pricing Batch: "
								+ path + ".accountNumber" + context
								+ "; duplicates accounts[" + duplicate + "].accountNumber");
			}

			for (int attributeIndex = 0; attributeIndex < account.getAttributes().size();
					attributeIndex++) {
				if (account.getAttributes().get(attributeIndex) == null) {
					throw new IllegalArgumentException(
							"Account Attributes must not contain null items: "
									+ path + ".attributes[" + attributeIndex + "]" + context);
				}
			}

			validateFees(account, path);
		}
	}

	private void validateFees(BatchAccount account, String accountPath) {
		Map<Long, Integer> feeRequestIds = new HashMap<>();
		for (int index = 0; index < account.getFees().size(); index++) {
			BatchFeeRequest fee = account.getFees().get(index);
			String path = accountPath + ".fees[" + index + "]";
			String context = " (accountNumber=" + account.getAccountNumber();
			if (fee == null) {
				throw new IllegalArgumentException(
						"Fee Requests must not contain null items: " + path + context + ")");
			}

			context += ", feeRequestId=" + fee.getFeeRequestId() + ", code=" + fee.getCode() + ")";
			Integer duplicate = feeRequestIds.putIfAbsent(fee.getFeeRequestId(), index);
			if (duplicate != null) {
				throw new IllegalArgumentException(
						"Fee Request IDs must be unique within an account: "
								+ path + ".feeRequestId" + context + "; duplicates "
								+ accountPath + ".fees[" + duplicate + "].feeRequestId");
			}

			if (fee.getTransactionAmount() != null
					&& fee.getTransactionAmount().remainder(new BigDecimal("0.01")).signum() != 0) {
				throw new IllegalArgumentException("Transaction Amount must use increments of 0.01: "
						+ path + ".transactionAmount" + context);
			}
		}
	}
}

