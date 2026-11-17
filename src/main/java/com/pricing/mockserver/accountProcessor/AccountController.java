package com.pricing.mockServer.accountProcessor;

import com.pricing.mockServer.generated.api.BatchApi;
import com.pricing.mockServer.generated.model.BatchAcceptance;
import com.pricing.mockServer.generated.model.BatchRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AccountController implements BatchApi {

	private final AccountService accountService;

	public AccountController(AccountService accountService) {
		this.accountService = accountService;
	}

	@Override
	public ResponseEntity<BatchAcceptance> postBatch(BatchRequest request) {
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(accountService.accept(request));
	}
}
