package com.pricing.mockServer.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration(proxyBeanMethods = false)
public class AccountStoreConfiguration {

	@Bean({"dataSource", "accountDataSource"})
	@Primary
	@ConfigurationProperties("spring.datasource.hikari")
	public HikariDataSource accountDataSource(DataSourceProperties properties) {
		HikariDataSource dataSource = properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
		dataSource.setMaximumPoolSize(5);
		return dataSource;
	}
}
