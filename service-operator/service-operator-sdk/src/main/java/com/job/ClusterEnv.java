package com.job;


public class ClusterEnv {

  private long id;
  private String cluster;
  private String env;
  private String key;
  private String value;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getEnv() {
    return env;
  }

  public void setEnv(String env) {
    this.env = env;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "ClusterEnv{" +
            "id=" + id +
            ", cluster='" + cluster + '\'' +
            ", env='" + env + '\'' +
            ", key='" + key + '\'' +
            ", value='" + value + '\'' +
            '}';
  }
}
