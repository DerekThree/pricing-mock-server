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
			try {
				synchronized (session) {
					session.sendMessage(new TextMessage(json.writeValueAsString(result)));
				}
			} catch (IOException ex) {
				log.warn("Failed to deliver Transaction Batch {}", result.batchId(), ex);
			}
		}
	}
}
