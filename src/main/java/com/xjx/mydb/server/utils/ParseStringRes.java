package com.xjx.mydb.server.utils;

/**
 * @Author: Xjx
 * @Create: 2023/2/8 - 17:29
 */
public class ParseStringRes {
    //当前字符串解析结果
    public String str;
    //下一个字符串的地址
    public int next;

    public ParseStringRes(String str, int next) {
        this.str = str;
        this.next = next;
    }
}
