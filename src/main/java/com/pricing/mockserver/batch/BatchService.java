package com.pricing.mockserver.batch;

import com.pricing.mockserver.generated.model.BatchAcceptance;
import com.pricing.mockserver.generated.model.BatchRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class BatchService {

	private final BatchRepository batchRepository;
	private final BatchMapper mapper;
	private final BatchValidator validator;

	public BatchService(
			BatchRepository batchRepository, BatchMapper mapper, BatchValidator validator) {
		this.batchRepository = batchRepository;
		this.mapper = mapper;
		this.validator = validator;
	}

	@Transactional
	public BatchAcceptance accept(BatchRequest request) {
		validator.validate(request);
		batchRepository.save(mapper.toBatchOutboxEntity(request));
		return new BatchAcceptance(request.getBatchId(), "Pricing Batch accepted");
	}
}
