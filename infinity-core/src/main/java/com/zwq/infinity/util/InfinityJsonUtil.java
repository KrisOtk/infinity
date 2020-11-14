package com.zwq.infinity.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InfinityJsonUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ObjectWriter writer = objectMapper.writer();
    private static final ObjectReader reader = objectMapper.reader();


    /**
     * 将object解析成json
     *
     * @param object
     * @return
     */
    public static String toJSON(Object object) {
        try {
            return writer.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 将json解析为对象
     *
     * @param json
     * @param target
     * @param <T>
     * @return
     */
    public static <T> T parse(String json, Class<T> target) {
        try {
            return objectMapper.readValue(json, target);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 解析list类型的json
     *
     * @param json
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> List<T> parse2List(String json, Class<T> clazz) {
        try {
            JavaType javaType = getCollectionType(ArrayList.class, clazz);
            return objectMapper.readValue(json, javaType);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 获取泛型的Collection Type
     *
     * @param collectionClass 泛型的Collection
     * @param elementClasses  元素类
     * @return JavaType Java类型
     * @since 1.0
     */
    public static JavaType getCollectionType(Class<?> collectionClass, Class<?>... elementClasses) {
        return objectMapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
    }
}
