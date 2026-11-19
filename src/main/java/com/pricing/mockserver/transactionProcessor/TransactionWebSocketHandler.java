package com.pricing.mockServer.transactionProcessor;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.json.JsonMapper;

@Component
public class TransactionWebSocketHandler extends TextWebSocketHandler {

	private static final Logger log = LoggerFactory.getLogger(TransactionWebSocketHandler.class);
	private final JsonMapper json;
	private volatile WebSocketSession dashboard;

	public TransactionWebSocketHandler(JsonMapper json) {
		this.json = json;
	}

	@Override
	public void afterConnectionEstablished(WebSocketSession session) {
		dashboard = session;
	}

	public void send(TransactionBatch result) {
		WebSocketSession session = dashboard;
		if (session != null && session.isOpen()) {
			TextMessage message = new TextMessage(json.writeValueAsString(result));
			try {
				synchronized (session) {
					session.sendMessage(message);
				}
				log.info("Pricing Batch sent: {}", result.batchId());
			} catch (IOException | IllegalStateException ex) {
				log.warn("Failed to deliver Transaction Batch {}", result.batchId(), ex);
			}
		}
	}
}
