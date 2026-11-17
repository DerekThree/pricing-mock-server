package com.pricing.mockServer.transactionProcessor;

import com.pricing.mockServer.generated.api.TransactionApi;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TransactionConrtoller implements TransactionApi {

	private final TransactionService transactionService;

	public TransactionConrtoller(TransactionService transactionService) {
		this.transactionService = transactionService;
	}

	@Override
	public ResponseEntity<Void> wakeUp() {
		transactionService.wakeUp();
		return ResponseEntity.status(HttpStatus.ACCEPTED).build();
	}
}
