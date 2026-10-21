package com.pricing.mockserver.config;

import java.math.BigDecimal;

import com.pricing.mockserver.accountattribute.AccountAttributeScalarValue;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;

public class AccountAttributeValueSerializer extends ValueSerializer<AccountAttributeScalarValue> {

	@Override
	public void serialize(AccountAttributeScalarValue value, JsonGenerator generator,
			SerializationContext context) {
		Object scalar = value.getValue();
		if (scalar instanceof String stringValue) {
			generator.writeString(stringValue);
			return;
		}

		if (scalar instanceof BigDecimal decimalValue) {
			generator.writeNumber(decimalValue);
			return;
		}

		if (scalar instanceof Boolean booleanValue) {
			generator.writeBoolean(booleanValue);
			return;
		}

		throw new IllegalArgumentException("Condition value must be a string, number, or boolean");
	}
}

