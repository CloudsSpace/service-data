package com.entity.funnel;

import lombok.*;

import java.util.List;

/**
 * @Author: ysh
 * @Date: 2019/10/18 15:37
 * @Version: 1.0
 */
@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Funnel {

    private Integer id;
    private String name;
    private List<String> stepName;
    private List<Filters> steps;
    private Integer project;


}
