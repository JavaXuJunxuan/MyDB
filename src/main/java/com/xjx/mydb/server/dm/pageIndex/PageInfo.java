package com.xjx.mydb.server.dm.pageIndex;

/**
 * @Author: Xjx
 * @Create: 2023/1/6 - 15:21
 * 页面信息类，存储页号和这个页号对应数据页的空闲空间大小
 */
public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
