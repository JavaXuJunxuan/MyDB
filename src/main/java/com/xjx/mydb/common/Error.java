package com.xjx.mydb.common;

/**
 * @Author: Xjx
 * @Create: 2022/12/24 - 13:03
 */
public class Error {
    //common
    public static final Exception CacheFullException = new RuntimeException("Cache is full!");
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
    public static final Exception FileCannotRWExcepiton = new RuntimeException("File cannot read or write!");

    //tm
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");

    //dm
    public static final Exception BadLogFileException = new RuntimeException("Bad log file!");
    public static final Exception MemTooSmallException = new RuntimeException("Memory too small!");
    public static final Exception DataTooLargeException = new RuntimeException("Data too large!");
    public static final Exception DatabaseBusyException = new RuntimeException("Database is busy!");

}
