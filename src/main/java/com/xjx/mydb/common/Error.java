package com.xjx.mydb.common;

/**
 * @Author: Xjx
 * @Create: 2022/12/24 - 13:03
 */
public class Error {
    //common
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
    public static final Exception FileCannotRWExcepiton = new RuntimeException("File cannot read or write!");

    //tm
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");
}
