package com.pricing.mockserver.config;

import java.sql.SQLException;
import java.util.Locale;
import java.util.stream.Collectors;

import com.pricing.mockserver.generated.model.ErrorResponse;
import jakarta.persistence.PersistenceException;
import jakarta.validation.ConstraintViolationException;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.transaction.TransactionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;
import tools.jackson.core.JacksonException;
import tools.jackson.core.TokenStreamLocation;

@RestControllerAdvice
public class GlobalExceptionHandler {

	private static final String BATCH_OUTBOX_PRIMARY_KEY = "batch_outbox_pkey";
	private static final String ACCOUNT_NUMBER_UNIQUE_CONSTRAINT = "uk_accounts_batch_account_number";
	private static final String FEE_REQUEST_ID_UNIQUE_CONSTRAINT =
			"uk_fee_requests_account_fee_request_id";
	private static final String ATTRIBUTE_SCALAR_CONSTRAINT = "chk_account_attributes_scalar";
	private static final String TRANSACTION_AMOUNT_CONSTRAINT = "chk_fee_requests_transaction_amount";
	private static final String UNIQUE_VIOLATION = "23505";
	private static final String CHECK_VIOLATION = "23514";

	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<ErrorResponse> handleMethodArgumentNotValid(
			MethodArgumentNotValidException ex) {
		String message = ex.getBindingResult().getFieldErrors().stream()
				.map(fieldError -> fieldError.getField() + " " + fieldError.getDefaultMessage())
				.collect(Collectors.joining(", "));
		return badRequest(message.isBlank() ? "Request validation failed" : message);
	}

	@ExceptionHandler(ConstraintViolationException.class)
	public ResponseEntity<ErrorResponse> handleConstraintViolation(ConstraintViolationException ex) {
		String message = ex.getConstraintViolations().stream()
				.map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
				.collect(Collectors.joining(", "));
		return badRequest(message.isBlank() ? "Request validation failed" : message);
	}

	@ExceptionHandler(HttpMessageNotReadableException.class)
	public ResponseEntity<ErrorResponse> handleHttpMessageNotReadable(
			HttpMessageNotReadableException ex) {
		Throwable cause = ex.getCause();
		while (cause != null) {
			if (cause instanceof JacksonException jacksonException) {
				StringBuilder path = new StringBuilder();
				for (JacksonException.Reference reference : jacksonException.getPath()) {
					if (reference.getPropertyName() != null) {
						if (!path.isEmpty()) {
							path.append('.');
						}
						path.append(reference.getPropertyName());
					} else if (reference.getIndex() >= 0) {
						path.append('[').append(reference.getIndex()).append(']');
					}
				}
				String message = "Request body is malformed" + (path.isEmpty() ? "" : " at " + path);
				TokenStreamLocation location = jacksonException.getLocation();
				if (location != null && location.getLineNr() > 0) {
					message += " (line " + location.getLineNr()
							+ ", column " + location.getColumnNr() + ")";
				}
				return badRequest(message);
			}
			cause = cause.getCause();
		}
		return badRequest("Request body is malformed");
	}

	@ExceptionHandler(IllegalArgumentException.class)
	public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex) {
		return badRequest(ex.getMessage());
	}

	@ExceptionHandler(DataIntegrityViolationException.class)
	public ResponseEntity<ErrorResponse> handleDataIntegrityViolation(
			DataIntegrityViolationException ex) {
		if (hasConstraint(ex, BATCH_OUTBOX_PRIMARY_KEY, UNIQUE_VIOLATION)) {
			return ResponseEntity.status(HttpStatus.CONFLICT)
					.body(new ErrorResponse("BATCH_ALREADY_ACCEPTED", databaseMessage(ex,
							"This Pricing Batch has already been accepted")));
		}

		if (hasConstraint(ex, ACCOUNT_NUMBER_UNIQUE_CONSTRAINT, UNIQUE_VIOLATION)) {
			return badRequest(databaseMessage(ex,
					"Account numbers must be unique within the Pricing Batch"));
		}

		if (hasConstraint(ex, FEE_REQUEST_ID_UNIQUE_CONSTRAINT, UNIQUE_VIOLATION)) {
			return badRequest(databaseMessage(ex,
					"Fee Request IDs must be unique within an account"));
		}

		if (hasConstraint(ex, ATTRIBUTE_SCALAR_CONSTRAINT, CHECK_VIOLATION)) {
			return badRequest(databaseMessage(ex,
					"Account Attribute value must be a string, number, or boolean"));
		}

		if (hasConstraint(ex, TRANSACTION_AMOUNT_CONSTRAINT, CHECK_VIOLATION)) {
			return badRequest(databaseMessage(ex,
					"Transaction Amount must be nonnegative and use increments of 0.01"));
		}

		return badRequest(databaseMessage(ex, "The request violates a database constraint"));
	}

	@ExceptionHandler({DataAccessException.class, TransactionException.class,
			PersistenceException.class})
	public ResponseEntity<ErrorResponse> handleAccountStoreUnavailable() {
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
				.body(new ErrorResponse("ACCOUNT_STORE_UNAVAILABLE",
						"The Account Store is unavailable. Please try again later."));
	}

	@ExceptionHandler(NoResourceFoundException.class)
	public ResponseEntity<ErrorResponse> handleNoResourceFound() {
		return ResponseEntity.status(HttpStatus.NOT_FOUND)
				.body(new ErrorResponse("NOT_FOUND", "Endpoint was not found"));
	}

	@ExceptionHandler(Exception.class)
	public ResponseEntity<ErrorResponse> handleUnexpected(Exception ex) {
		return ResponseEntity.internalServerError()
				.body(new ErrorResponse("INTERNAL_ERROR", ex.getMessage() == null
						? "An unexpected error occurred" : ex.getMessage()));
	}

	private ResponseEntity<ErrorResponse> badRequest(String message) {
		return ResponseEntity.badRequest().body(new ErrorResponse("INVALID_BATCH", message));
	}

	private String databaseMessage(Throwable exception, String message) {
		Throwable cause = exception;
		while (cause != null) {
			if (cause instanceof PSQLException postgresException
					&& postgresException.getServerErrorMessage() != null) {
				ServerErrorMessage error = postgresException.getServerErrorMessage();
				String table = error.getTable();
				String column = error.getColumn();
				String context = table == null ? "" : " [" + table
						+ (column == null ? "" : "." + column) + "]";
				return message + context + (error.getDetail() == null ? "" : ": " + error.getDetail());
			}
			cause = cause.getCause();
		}
		return message;
	}

	private boolean hasConstraint(Throwable exception, String constraint, String... states) {
		Throwable cause = exception;
		while (cause != null) {
			if (cause instanceof SQLException sqlException && sqlException.getMessage() != null
					&& sqlException.getMessage().toLowerCase(Locale.ROOT).contains(constraint)) {
				for (String state : states) {
					if (state.equals(sqlException.getSQLState())) {
						return true;
					}
				}
			}
			cause = cause.getCause();
		}

		return false;
	}
}
