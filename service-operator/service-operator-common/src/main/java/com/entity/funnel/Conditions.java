package com.entity.funnel;

import lombok.*;

import java.util.List;

@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Conditions {

    private List<Condition> conditions;
    private String relation;

}