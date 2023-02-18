package com.xjx.mydb.server.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @Author: Xjx
 * @Create: 2022/12/27 - 15:24
 */
public class RandomUtil {
    //根据参数传入的长度随机生成一个字节数组
    public static byte[] randomBytes(int length) {
        Random random = new SecureRandom();
        byte[] buf = new byte[length];
        random.nextBytes(buf);
        return buf;
    }
}
