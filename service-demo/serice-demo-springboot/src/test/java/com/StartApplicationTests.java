package com;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class StartApplicationTests {

    @Test
    void contextLoads() {
        System.out.println("word");
    };
    @Test
     String demo(){
        return "测试";
    }

}
