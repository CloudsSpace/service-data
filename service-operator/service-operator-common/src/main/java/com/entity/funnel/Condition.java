package com.entity.funnel;

import lombok.*;

import java.util.List;

@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Condition {

    private String field;
    private String function;
    private List<String> params;

}