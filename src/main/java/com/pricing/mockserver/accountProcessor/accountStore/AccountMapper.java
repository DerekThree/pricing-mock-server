package com.pricing.mockServer.accountProcessor.accountStore;

import com.pricing.mockServer.generated.model.AccountAttribute;
import com.pricing.mockServer.generated.model.BatchAccount;
import com.pricing.mockServer.generated.model.BatchFeeRequest;
import com.pricing.mockServer.generated.model.BatchRequest;
import org.springframework.stereotype.Component;

@Component
public
class AccountMapper {

	public AccountOutboxEntity toAccountOutboxEntity(BatchRequest request) {
		AccountOutboxEntity batch = AccountOutboxEntity.builder()
				.batchId(request.getBatchId()).processed(false).build();
		batch.setAccounts(request.getAccounts().stream()
				.map(account -> toAccountEntity(account, batch)).toList());
		return batch;
	}

	private AccountEntity toAccountEntity(BatchAccount submitted, AccountOutboxEntity batch) {
		AccountEntity account = AccountEntity.builder()
				.batch(batch)
				.accountNumber(submitted.getAccountNumber())
				.productCode(submitted.getProductCode())
				.branchCode(submitted.getBranchCode())
				.pricingDate(submitted.getPricingDate())
				.build();
		account.setAttributes(submitted.getAttributes().stream()
				.map(attribute -> toAccountAttributeEntity(attribute, account)).toList());
		account.setFees(submitted.getFees().stream()
				.map(fee -> toFeeRequestEntity(fee, account)).toList());
		return account;
	}

	private AccountAttributeEntity toAccountAttributeEntity(
			AccountAttribute attribute, AccountEntity account) {
		return AccountAttributeEntity.builder()
				.account(account)
				.code(attribute.getCode())
				.value((AccountAttributeScalarValue) attribute.getValue())
				.build();
	}

	private FeeRequestEntity toFeeRequestEntity(BatchFeeRequest fee, AccountEntity account) {
		return FeeRequestEntity.builder()
				.account(account)
				.feeRequestId(fee.getFeeRequestId())
				.code(fee.getCode())
				.transactionAmount(fee.getTransactionAmount())
				.build();
	}
}
