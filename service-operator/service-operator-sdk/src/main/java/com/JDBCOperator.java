package com;


import com.exception.EnvParamsException;
import com.job.AppParams;
import com.job.ClusterEnv;
import com.util.PropertiesUtil;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * @Author: ysh
 * @Date: 2019/7/1 17:47
 * @Version: 1.0
 */
public class JDBCOperator implements Serializable {

    private String driver;
    private String dbUrl;
    private String username;
    private String password;
    private String env;
    private Properties properties;
    private Connection connection;

    public JDBCOperator() {
        initialize();
    }

    public void initialize() {
        try {
            properties = PropertiesUtil.getProperties("jdbc.properties");
            driver = properties.getProperty("jdbc.driver");
            Class.forName(driver);
            dbUrl = properties.getProperty("jdbc.url");
            username = properties.getProperty("jdbc.username");
            password = properties.getProperty("jdbc.password");
            env = properties.getProperty("env");
            if (connection == null || connection.isClosed()) {
                connection = DriverManager.getConnection(dbUrl, username, password);  //连接数据库
            }
            System.out.println("当前运行环境 : " + env);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取任务执行参数
     *
     * @param clazz
     * @return
     * @throws SQLException
     * @throws EnvParamsException
     */
    public Map<String, HashMap<String, String>> getAppParams(Class clazz) throws EnvParamsException {
        PreparedStatement preparedStatement = null;
        Map<String, HashMap<String, String>> map = null;
        try {
            preparedStatement = connection.prepareStatement("select * from app_params t1 where exists (select 1 from appinfo t2 where t1.a_id = t2.id and mainclass = '" + clazz.getName().replace("$", "") + "')");
            ResultSet resultSet = preparedStatement.executeQuery();
            ArrayList<AppParams> appParams = putResult(resultSet, AppParams.class);
            if (appParams == null) {
                throw new EnvParamsException("app params is null");
            }
            map = new HashMap<>();
            for (AppParams appParam : appParams) {
                HashMap<String, String> hashMap = map.get(appParam.getType());
                if (hashMap == null) {
                    hashMap = new HashMap<>();
                }
                hashMap.put(appParam.getKey(), appParam.getValue());
                map.put(appParam.getType(), hashMap);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 获取集群环境
     *
     * @param clusterName
     * @return
     * @throws SQLException
     * @throws EnvParamsException
     */
    public String getClusterEnv(String clusterName) throws EnvParamsException {
        String nodes = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("select * from bidb.cluster_env where env='" + env + "' and cluster = '" + clusterName + "'");
            ResultSet resultSet = preparedStatement.executeQuery();
            ArrayList<ClusterEnv> clusterEnvs = putResult(resultSet, ClusterEnv.class);
            if (clusterEnvs == null) {
                throw new EnvParamsException("cluster params is null");
            }
            ClusterEnv clusterEnv = clusterEnvs.get(0);
            nodes = clusterEnv.getValue();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return nodes;
    }


    public Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(dbUrl, username, password);  //连接数据库
        }
        return connection;
    }


    /**
     * 把ResultSet的结果放到java对象中
     *
     * @param <T>
     * @param rs  ResultSet
     * @param obj java类的class
     * @return
     */
    public <T> ArrayList<T> putResult(ResultSet rs, Class<T> obj) {
        try {
            ArrayList<T> arrayList = new ArrayList<T>();
            ResultSetMetaData metaData = rs.getMetaData();
            /**
             * 获取总列数
             */
            int count = metaData.getColumnCount();
            while (rs.next()) {
                /**
                 * 创建对象实例
                 */
                T newInstance = obj.newInstance();
                for (int i = 1; i <= count; i++) {
                    /**
                     * 给对象的某个属性赋值
                     */
                    String name = metaData.getColumnName(i).toLowerCase();
                    name = toJavaField(name);// 改变列名格式成java命名格式
                    String substring = name.substring(0, 1);// 首字母大写
                    String replace = name.replaceFirst(substring, substring.toUpperCase());
                    Class<?> type = null;
                    try {
                        type = obj.getDeclaredField(name).getType();// 获取字段类型
                    } catch (NoSuchFieldException e) { // Class对象未定义该字段时,跳过
                        continue;
                    }

                    Method method = obj.getMethod("set" + replace, type);
                    /**
                     * 判断读取数据的类型
                     */
                    if (type.isAssignableFrom(String.class)) {
                        method.invoke(newInstance, rs.getString(i));
                    } else if (type.isAssignableFrom(byte.class) || type.isAssignableFrom(Byte.class)) {
                        method.invoke(newInstance, rs.getByte(i));// byte 数据类型是8位、有符号的，以二进制补码表示的整数
                    } else if (type.isAssignableFrom(short.class) || type.isAssignableFrom(Short.class)) {
                        method.invoke(newInstance, rs.getShort(i));// short 数据类型是 16 位、有符号的以二进制补码表示的整数
                    } else if (type.isAssignableFrom(int.class) || type.isAssignableFrom(Integer.class)) {
                        method.invoke(newInstance, rs.getInt(i));// int 数据类型是32位、有符号的以二进制补码表示的整数
                    } else if (type.isAssignableFrom(long.class) || type.isAssignableFrom(Long.class)) {
                        method.invoke(newInstance, rs.getLong(i));// long 数据类型是 64 位、有符号的以二进制补码表示的整数
                    } else if (type.isAssignableFrom(float.class) || type.isAssignableFrom(Float.class)) {
                        method.invoke(newInstance, rs.getFloat(i));// float 数据类型是单精度、32位、符合IEEE 754标准的浮点数
                    } else if (type.isAssignableFrom(double.class) || type.isAssignableFrom(Double.class)) {
                        method.invoke(newInstance, rs.getDouble(i));// double 数据类型是双精度、64 位、符合IEEE 754标准的浮点数
                    } else if (type.isAssignableFrom(BigDecimal.class)) {
                        method.invoke(newInstance, rs.getBigDecimal(i));
                    } else if (type.isAssignableFrom(boolean.class) || type.isAssignableFrom(Boolean.class)) {
                        method.invoke(newInstance, rs.getBoolean(i));// boolean数据类型表示一位的信息
                    } else if (type.isAssignableFrom(Date.class)) {
                        method.invoke(newInstance, rs.getDate(i));
                    } else if (type.isAssignableFrom(Timestamp.class)) {
                        method.invoke(newInstance, rs.getTimestamp(i));
                    }
                }
                arrayList.add(newInstance);
            }
            return arrayList;

        } catch (InstantiationException | IllegalAccessException | SQLException | SecurityException | NoSuchMethodException | IllegalArgumentException
                | InvocationTargetException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 数据库命名格式转java命名格式
     *
     * @param str 数据库字段名
     * @return java字段名
     */
    public static String toJavaField(String str) {

        String[] split = str.split("_");
        StringBuilder builder = new StringBuilder();
        builder.append(split[0]);// 拼接第一个字符

        // 如果数组不止一个单词
        if (split.length > 1) {
            for (int i = 1; i < split.length; i++) {
                // 去掉下划线，首字母变为大写
                String string = split[i];
                String substring = string.substring(0, 1);
                split[i] = string.replaceFirst(substring, substring.toUpperCase());
                builder.append(split[i]);
            }
        }

        return builder.toString();
    }

    public Properties getProperties() {
        return properties;
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void close(Connection connection, PreparedStatement preparedStatement) {
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String getDriver() {
        return driver;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
