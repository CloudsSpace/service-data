package com.entity.funnel;
import lombok.*;

@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Filters {

    private Conditions filter;
    private String customName;
    private String eventName;


}