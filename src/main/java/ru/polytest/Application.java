package ru.polytest;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
public class Application {

    public static void main(String[] args) throws KeyManagementException 
    {
        SpringApplication.run(Application.class, args);        
    }

    @Bean
    public ApplicationRunner startupRunner(StakanService stakanService) {
        return args -> {
            // Force eager initialization of the service even with global lazy-init enabled.
        };
    }
}
