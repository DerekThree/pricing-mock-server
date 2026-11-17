package com.pricing.mockserver.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class CorsConfig implements WebMvcConfigurer {

	private final String dashboardOrigin;

	public CorsConfig(@Value("${dashboard.origin:http://localhost:5173}") String dashboardOrigin) {
		this.dashboardOrigin = dashboardOrigin;
	}

	@Override
	public void addCorsMappings(CorsRegistry registry) {
		registry.addMapping("/**")
				.allowedOrigins(dashboardOrigin)
				.allowedOriginPatterns("http://localhost:*", "http://127.0.0.1:*")
				.allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
				.allowedHeaders("*")
				.allowCredentials(true);
	}
}
