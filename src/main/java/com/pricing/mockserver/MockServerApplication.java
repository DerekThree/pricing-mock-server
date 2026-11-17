package com.pricing.mockServer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;

import com.pricing.mockServer.accountProcessor.accountStore.AccountEntity;

@SpringBootApplication
@EntityScan(basePackageClasses = AccountEntity.class)
public class MockServerApplication {

	public static void main(String[] args) {
		SpringApplication.run(MockServerApplication.class, args);
	}

}
