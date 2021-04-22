package com;

import com.exception.EnvParamsException;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

public class JedisClusterOperator {

    public static JedisCluster getJedisClusterInstance() {
        return SingletonJedisCluster.INSTANCE;
    }

    private static class SingletonJedisCluster {

        private static JedisCluster INSTANCE = initJedisCluster();

        public static JedisCluster initJedisCluster() {
            JedisCluster jedisCluster = null;
            try {
                JDBCOperator jdbcOperator = new JDBCOperator();
                String nodes = jdbcOperator.getClusterEnv("redis");
                Set<HostAndPort> clusterNodes = new HashSet<>();
                String[] split = nodes.split(",");
                for (int i = 0; i < split.length; i++) {
                    initClusterNodes(clusterNodes, split[i]);
                }
                jedisCluster = new JedisCluster(clusterNodes, 60000, 60000, 5, initJedisPoolConfig());
            } catch (EnvParamsException e) {
                e.printStackTrace();
            }
            return jedisCluster;
        }
    }

    private static void initClusterNodes(Set<HostAndPort> clusterNodes, String hostAndPortStr) {
        HostAndPort hostAndPort = HostAndPort.parseString(hostAndPortStr);
        clusterNodes.add(hostAndPort);
    }

    private static JedisPoolConfig initJedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //jedis连接池中最大的连接个数
        jedisPoolConfig.setMaxTotal(20);
        //jedis连接池中最大的空闲连接个数
        jedisPoolConfig.setMaxIdle(10);
        //jedis连接池中最小的空闲连接个数
        jedisPoolConfig.setMinIdle(5);
        //jedis连接池最大的等待连接时间 ms值
        jedisPoolConfig.setMaxWaitMillis(10000);

        return jedisPoolConfig;
    }

}
