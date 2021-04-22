package com.entity.mysqlstatic;
import lombok.*;

@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class MysqlSyncStaticFields {

  private long id;
  private long fieldId;
  private String name;
  private long type;

}
