package com.song.cn.concurrent.threadlocal;

import com.song.cn.common.Utils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class CommonMistakesApplication {

    public static void main(String[] args) {
        Utils.loadPropertySource(CommonMistakesApplication.class, "tomcat.properties");

        SpringApplication.run(CommonMistakesApplication.class, args);
    }
}

