package de.bschwering.edi_workshop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class EdiWorkshopApplication {

	public static void main(String[] args) {
		SpringApplication.run(EdiWorkshopApplication.class, args);
	}

}
