package com.temp.bitcask.bitcask.test;

import com.temp.bitcask.bitcask.request.PutKeyRequest;
import org.springframework.http.HttpEntity;
import org.springframework.web.client.RestTemplate;

import java.util.Random;

public class DataDumper {

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    public static void main(String[] args) {
        RestTemplate restTemplate = new RestTemplate();
        for(int i =0;i<1000;i++){
            HttpEntity<PutKeyRequest> request = new HttpEntity<>(new PutKeyRequest(generateRandomString(10), generateRandomString(100)));
            restTemplate.postForObject("http://localhost:8080/putKey", request, Object.class);
        }
    }

    public static String generateRandomString(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int randomIndex = random.nextInt(CHARACTERS.length());
            char randomChar = CHARACTERS.charAt(randomIndex);
            sb.append(randomChar);
        }

        return sb.toString();
    }
}
