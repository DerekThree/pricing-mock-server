package com.pricing.mockserver.accountattribute;

import java.math.BigDecimal;
import java.util.Objects;

import com.pricing.mockserver.generated.model.AccountAttributeValue;

public final class AccountAttributeScalarValue implements AccountAttributeValue {

	private final Object value;

	public AccountAttributeScalarValue(Object value) {
		if (!(value instanceof String) && !(value instanceof BigDecimal) && !(value instanceof Boolean)) {
			throw new IllegalArgumentException("Condition value must be a string, number, or boolean");
		}

		this.value = value;
	}

	public Object getValue() {
		return value;
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) {
			return true;
		}

		if (!(object instanceof AccountAttributeScalarValue other)) {
			return false;
		}

		return Objects.equals(value, other.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value);
	}

	@Override
	public String toString() {
		return String.valueOf(value);
	}
}

