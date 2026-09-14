package com.gokgor.logworm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LogwormApplication {

	public static void main(String[] args) {
		SpringApplication.run(LogwormApplication.class, args);
	}

}
