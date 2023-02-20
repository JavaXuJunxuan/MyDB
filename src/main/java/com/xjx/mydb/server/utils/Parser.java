package com.xjx.mydb.server.utils;

import java.nio.ByteBuffer;

/**
 * @Author: Xjx
 * @Create: 2023/2/22 - 21:12
 */
public class Parser {

    //分配一个大小等于long类型的字节缓冲区然后把值放进去，返回一个字节数组
    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    //讲一个字节数组中的数据解析为一个long类型的数据
    public static long parseLong(byte[] buf){
        //使用字节缓冲读取等于long类型大小的字节数据然后返回long类型的数据即可
        ByteBuffer buffer = ByteBuffer.wrap(buf,0,8);
        return buffer.getLong();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static short parseShort(byte[] buf){
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }


}
