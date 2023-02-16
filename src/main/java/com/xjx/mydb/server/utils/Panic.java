package com.xjx.mydb.server.utils;

/**
 * @Author: Xjx
 * @Create: 2022/12/22 - 13:01
 */
public class Panic {
    public static void panic(Exception e){
        e.printStackTrace();
        System.exit(1);
    }
}
