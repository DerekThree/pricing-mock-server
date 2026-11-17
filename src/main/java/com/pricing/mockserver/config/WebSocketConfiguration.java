package com.pricing.mockServer.config;

import com.pricing.mockServer.transactionProcessor.TransactionWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration(proxyBeanMethods = false)
@EnableWebSocket
public class WebSocketConfiguration implements WebSocketConfigurer {

	private final TransactionWebSocketHandler transactionWebSocketHandler;

	public WebSocketConfiguration(TransactionWebSocketHandler transactionWebSocketHandler) {
		this.transactionWebSocketHandler = transactionWebSocketHandler;
	}

	@Override
	public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
		registry.addHandler(transactionWebSocketHandler, "/api/v1/mock/txn/batches")
				.setAllowedOriginPatterns("http://localhost:*", "http://127.0.0.1:*");
	}
}
