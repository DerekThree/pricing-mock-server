package com.pricing.mockserver.config;

import com.pricing.mockserver.accountattribute.AccountAttributeScalarValue;
import com.pricing.mockserver.generated.model.AccountAttributeValue;
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

