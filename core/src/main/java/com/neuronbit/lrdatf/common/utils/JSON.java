package com.neuronbit.lrdatf.common.utils;

import com.google.gson.Gson;

public class JSON {
    private static final Gson gson = new Gson();


    public static String toJSONString(Object obj) {
        return gson.toJson(obj);
    }

    public static <T> T parseObject(String json, Class<T> classOfT) {
        return gson.fromJson(json, classOfT);
    }
}
