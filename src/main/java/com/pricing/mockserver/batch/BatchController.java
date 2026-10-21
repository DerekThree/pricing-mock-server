package com.pricing.mockserver.batch;

import com.pricing.mockserver.generated.api.BatchApi;
import com.pricing.mockserver.generated.model.BatchAcceptance;
import com.pricing.mockserver.generated.model.BatchRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BatchController implements BatchApi {

	private final BatchService batchService;

	public BatchController(BatchService batchService) {
		this.batchService = batchService;
	}

	@Override
	public ResponseEntity<BatchAcceptance> postBatch(BatchRequest request) {
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(batchService.accept(request));
	}
}
