package com.atguigu.userprofile.utils;

import redis.clients.jedis.Jedis;

import java.util.Set;

public class TestUtil {

    public static void main(String[] args) {

        Jedis jedis = new Jedis("bigdata01",6379);
        jedis.auth("123");
        jedis.set("k1000","v1000");
        Set<String> keys = jedis.keys("*");
        for (String key : keys) {
            System.out.println(key);
        }
        jedis.close();




    }
}
