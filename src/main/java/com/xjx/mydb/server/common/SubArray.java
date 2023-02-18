package com.xjx.mydb.server.common;

/**
 * @Author: Xjx
 * @Create: 2022/12/22 - 12:18
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end){
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
