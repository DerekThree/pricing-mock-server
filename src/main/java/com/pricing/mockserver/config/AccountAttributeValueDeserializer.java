package com.pricing.mockserver.config;

import java.math.BigDecimal;

import com.pricing.mockserver.accountattribute.AccountAttributeScalarValue;
import com.pricing.mockserver.generated.model.AccountAttributeValue;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.exc.InvalidFormatException;

public class AccountAttributeValueDeserializer extends ValueDeserializer<AccountAttributeValue> {

	@Override
	public AccountAttributeValue deserialize(JsonParser parser, DeserializationContext context) {
		JsonToken token = parser.currentToken();
		if (token == JsonToken.VALUE_STRING) {
			return new AccountAttributeScalarValue(parser.getText());
		}

		if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
			return new AccountAttributeScalarValue(parser.getDecimalValue());
		}

		if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) {
			return new AccountAttributeScalarValue(parser.getBooleanValue());
		}

		throw InvalidFormatException.from(parser,
				"Condition value must be a string, number, or boolean",
				parser.readValueAsTree(),
				Object.class);
	}
}

