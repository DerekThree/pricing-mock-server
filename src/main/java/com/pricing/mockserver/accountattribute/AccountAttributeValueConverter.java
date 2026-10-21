package com.pricing.mockserver.accountattribute;

import java.math.BigDecimal;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

@Converter
public class AccountAttributeValueConverter
		implements AttributeConverter<AccountAttributeScalarValue, String> {

	private static final JsonMapper JSON = JsonMapper.builder()
			.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
			.enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
			.build();

	@Override
	public String convertToDatabaseColumn(AccountAttributeScalarValue value) {
		return JSON.writeValueAsString(value.getValue());
	}

	@Override
	public AccountAttributeScalarValue convertToEntityAttribute(String value) {
		Object scalar = JSON.readValue(value, Object.class);
		if (scalar instanceof Number number) {
			scalar = new BigDecimal(number.toString());
		}
		return new AccountAttributeScalarValue(scalar);
	}
}
