package com.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
@RequestMapping("/admin")
@RestController
public class StratController {
    @RequestMapping("/demo")
    public String startSpringBoot() {
        return "Welcome to the world of Spring Boot Dev-Tools going!";
    }
}
