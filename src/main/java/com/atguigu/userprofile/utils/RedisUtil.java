package com.atguigu.userprofile.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Set;

public class RedisUtil {

    public static void main(String[] args) {
        //Jedis jedis = new Jedis("bigdata01",6379);
        Jedis jedis = RedisUtil.getJedis();
        jedis.set("k0428","v0428");

        jedis.close(); //关闭？ jedis会判断 来源于池 还是独立创建

    }

    static  JedisPool  jedisPool =initJedisPool();


    public static JedisPool initJedisPool(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(200); // 最大连接数
        jedisPoolConfig.setMaxIdle(30);// 最多维持30
        jedisPoolConfig.setMinIdle(10);// 至少维持10
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(5000);
        jedisPoolConfig.setTestOnBorrow(true); //借走连接时测试

        jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,60000);
        return  jedisPool;
    }


    public static Jedis getJedis() {

       return   jedisPool.getResource();
    }
}
