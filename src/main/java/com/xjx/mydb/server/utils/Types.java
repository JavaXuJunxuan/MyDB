package com.xjx.mydb.server.utils;

/**
 * @Author: Xjx
 * @Create: 2023/1/8 - 17:26
 */
public class Types {
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
