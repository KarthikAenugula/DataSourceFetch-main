package com.datasourcefetch.DataSourceFetch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
//@EnableBatchProcessing
@ComponentScan(basePackages = {"com.datasourcefetch"})
public class DataSourceFetchApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataSourceFetchApplication.class, args);
	}
}
