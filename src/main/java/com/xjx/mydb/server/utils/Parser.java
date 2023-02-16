package com.xjx.mydb.server.utils;

import java.nio.ByteBuffer;

/**
 * @Author: Xjx
 * @Create: 2023/2/22 - 21:12
 */
public class Parser {
    public static long parseLong(byte[] buf){
        ByteBuffer buffer = ByteBuffer.wrap(buf,0,8);
        return buffer.getLong();
    }

    //分配一个大小等于long类型的字节缓冲区然后把值放进去，返回一个字节数组
    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}
