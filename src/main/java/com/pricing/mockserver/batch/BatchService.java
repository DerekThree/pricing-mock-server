package com.pricing.mockserver.batch;

import java.time.Duration;

import com.pricing.mockserver.generated.model.BatchAcceptance;
import com.pricing.mockserver.generated.model.BatchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Service
public class BatchService {

	private static final Logger log = LoggerFactory.getLogger(BatchService.class);

	private final BatchRepository batchRepository;
	private final BatchMapper mapper;
	private final BatchValidator validator;
	private final RestClient pricingEngineClient;
	private final int maxAttempts;
	private final Duration retryDelay;

	public BatchService(
			BatchRepository batchRepository, BatchMapper mapper, BatchValidator validator,
			RestClient pricingEngineClient,
			@Value("${pricing-engine.max-attempts}") int maxAttempts,
			@Value("${pricing-engine.retry-delay}") Duration retryDelay) {
		Assert.isTrue(maxAttempts > 0, "Pricing Engine attempt limit must be positive");
		Assert.isTrue(!retryDelay.isNegative(), "Pricing Engine retry delay must not be negative");
		this.batchRepository = batchRepository;
		this.mapper = mapper;
		this.validator = validator;
		this.pricingEngineClient = pricingEngineClient;
		this.maxAttempts = maxAttempts;
		this.retryDelay = retryDelay;
	}

	public BatchAcceptance accept(BatchRequest request) {
		log.info("Pricing Batch received: {}", request.getBatchId());
		validator.validate(request);
		batchRepository.save(mapper.toBatchOutboxEntity(request));
		log.info("Pricing Batch stored: {}", request.getBatchId());
		Thread.startVirtualThread(this::notifyPricingEngine);
		return new BatchAcceptance(request.getBatchId(), "Pricing Batch accepted");
	}

	public void notifyPricingEngine() {
		for (int attempt = 1; attempt <= maxAttempts; attempt++) {
			String failure;
			try {
				HttpStatusCode status = pricingEngineClient.post().retrieve().toBodilessEntity()
						.getStatusCode();
				if (status.is2xxSuccessful()) {
					log.info("Pricing Engine wake-up acknowledged");
					return;
				}
				failure = "HTTP " + status.value();
			} catch (RestClientException ex) {
				failure = ex.getClass().getSimpleName() + ": " + ex.getMessage();
			}
			if (attempt == maxAttempts) {
				log.error("Final Pricing Engine wake-up attempt failed.  {}. Delivery stopped",
						failure);
				return;
			}
			log.warn("Pricing Engine wake-up attempt {} of {} failed. {}.", attempt, maxAttempts, failure);
			try {
				Thread.sleep(retryDelay);
			} catch (InterruptedException ex) {
				log.warn("Pricing Engine wake-up retry interrupted after attempt {} of {}. Delivery stopped",
						attempt, maxAttempts);
				Thread.currentThread().interrupt();
				return;
			}
		}
	}
}
