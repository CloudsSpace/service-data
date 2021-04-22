package com.redis.utils;

import java.lang.reflect.Field;

public class ReflectUtil {

    public static Field getField(Class<?> clazz, String fieldName) {
        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            if(!clazz.getSuperclass().isAssignableFrom(Object.class)){
                return getField(clazz.getSuperclass(),fieldName);
            }
            return null;
        }
    }

    public static <T> T getObject(Object obj, String fieldName) {
        try {
            Field field = getField(obj.getClass(), fieldName);
            field.setAccessible(true);
            return (T) field.get(obj);
        } catch (IllegalAccessException e) {
            return null;
        }
    }

}
