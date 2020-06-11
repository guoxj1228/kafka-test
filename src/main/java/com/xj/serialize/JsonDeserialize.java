package com.xj.serialize;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class JsonDeserialize implements Deserializer<JSONObject> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public JSONObject deserialize(String topic, byte[] data) {
        JSONObject obj = null;
        try {
            obj = JSON.parseObject(new String(data,"UTF-8"));
        } catch (JSONException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return obj;
    }



    @Override
    public void close() {
    }
}
