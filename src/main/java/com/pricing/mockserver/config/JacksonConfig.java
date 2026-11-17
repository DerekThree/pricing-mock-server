package com.pricing.mockServer.config;

import com.pricing.mockServer.accountProcessor.accountStore.AccountAttributeScalarValue;
import com.pricing.mockServer.generated.model.AccountAttributeValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.module.SimpleModule;

@Configuration
public class JacksonConfig {

	@Bean
	public SimpleModule AccountAttributeValueModule() {
		SimpleModule module = new SimpleModule();
		module.addDeserializer(AccountAttributeValue.class, new AccountAttributeValueDeserializer());
		module.addSerializer(AccountAttributeScalarValue.class,
				new AccountAttributeValueSerializer());
		return module;
	}
}

