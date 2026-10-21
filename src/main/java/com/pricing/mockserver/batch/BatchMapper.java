package com.pricing.mockserver.batch;

import com.pricing.mockserver.account.AccountEntity;
import com.pricing.mockserver.accountattribute.AccountAttributeEntity;
import com.pricing.mockserver.accountattribute.AccountAttributeScalarValue;
import com.pricing.mockserver.feerequest.FeeRequestEntity;
import com.pricing.mockserver.generated.model.AccountAttribute;
import com.pricing.mockserver.generated.model.BatchAccount;
import com.pricing.mockserver.generated.model.BatchFeeRequest;
import com.pricing.mockserver.generated.model.BatchRequest;
import org.springframework.stereotype.Component;

@Component
class BatchMapper {

	BatchOutboxEntity toBatchOutboxEntity(BatchRequest request) {
		BatchOutboxEntity batch = BatchOutboxEntity.builder()
				.batchId(request.getBatchId()).processed(false).build();
		batch.setAccounts(request.getAccounts().stream()
				.map(account -> toAccountEntity(account, batch)).toList());
		return batch;
	}

	private AccountEntity toAccountEntity(BatchAccount submitted, BatchOutboxEntity batch) {
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
