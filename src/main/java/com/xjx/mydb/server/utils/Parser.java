package com.xjx.mydb.server.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @Author: Xjx
 * @Create: 2022/12/24 - 21:12
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

    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4 + length));
        //+4是因为最前面有四个字节保存字符串长度
        return new ParseStringRes(str, length + 4);
    }

    //将字符串解析为字节数组，因为字符串长度都不确定，因此保存字符串的字节形式时都需要先用四个字节保存字符串的实际长度
    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    //将字符串根据一个公式解析成一个UID
    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }


}
