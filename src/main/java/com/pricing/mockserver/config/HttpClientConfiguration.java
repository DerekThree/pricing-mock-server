package com.pricing.mockserver.config;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;

@Configuration
@EnableAsync
public class HttpClientConfiguration {

	@Bean
	public RestClient pricingEngineClient(
			@Value("${pricing-engine.wake-up-url}") String wakeUpUrl,
			@Value("${pricing-engine.http-timeout}") Duration timeout) {
		Assert.isTrue(timeout.toMillis() > 0, "Pricing Engine HTTP timeout must be positive");
		SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
		factory.setConnectTimeout(timeout);
		factory.setReadTimeout(timeout);
		return RestClient.builder().baseUrl(wakeUpUrl).requestFactory(factory).build();
	}
}
