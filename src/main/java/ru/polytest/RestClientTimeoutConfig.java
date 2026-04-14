package ru.polytest;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class RestClientTimeoutConfig {

    @Bean
    public RestClient.Builder customRestClientBuilder() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(10_000); // 10 секунд на подключение
        factory.setReadTimeout(60_000);    // 60 секунд на ответ

        return RestClient.builder().requestFactory(factory);
    }
}
