package com.util;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

/**
 *  过期工具类
 */
public class C3p0Util {
    private static ComboPooledDataSource cpds;
    private static C3p0Util c3p0Util;


    /**
     * 构造方法初始化 配置文件
     */
    public C3p0Util() {
        if (cpds == null) {
            cpds = new ComboPooledDataSource();
        }
        //加载配置文件
        Properties props = new Properties();
        try {
            props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("db.properties"));
            cpds.setDriverClass(props.getProperty("mysqlDriver"));
            cpds.setJdbcUrl(props.getProperty("mysqlUrl"));
            cpds.setUser(props.getProperty("mysqlUser"));
            cpds.setPassword(props.getProperty("mysqlPassword"));

            cpds.setMaxPoolSize(Integer.parseInt(props.getProperty("MaxPoolSize")));
            cpds.setMinPoolSize(Integer.parseInt(props.getProperty("MinPoolSize")));
            cpds.setInitialPoolSize(Integer.parseInt(props.getProperty("InitialPoolSize")));

            cpds.setMaxIdleTime(Integer.parseInt(props.getProperty("MaxIdleTime")));
            cpds.setIdleConnectionTestPeriod(Integer.parseInt(props.getProperty("IdleConnectionTestPeriod")));
            //cpds.setAcquireRetryAttempts(Integer.parseInt(props.getProperty("AcquireRetryAttempts")));
            cpds.setTestConnectionOnCheckout(Boolean.parseBoolean(props.getProperty("TestConnectionOnCheckout")));
            cpds.setTestConnectionOnCheckin(Boolean.parseBoolean(props.getProperty("TestConnectionOnCheckin")));


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 返回连接池的实例
     *
     * @return
     */
    public static C3p0Util getInstance() {
        if(c3p0Util==null){
            c3p0Util = new C3p0Util();
        }
        return c3p0Util;
    }


    public Connection getConnection() {
        Connection conn = null;
        try {
            conn = cpds.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public void close() {
        if (cpds != null) {
            cpds.close();
        }
    }

    /**
     * 把ResultSet的结果放到java对象中
     *
     * @param <T>
     * @param rs  ResultSet
     * @param obj java类的class
     * @return
     */
    public static  <T> ArrayList<T> putResult(ResultSet rs, Class<T> obj) {
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

}