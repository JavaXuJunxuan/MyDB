package com.xjx.mydb.server.dm.page;

/**
 * @Author: Xjx
 * @Create: 2022/12/23 - 9:40
 */
//这是缓存数据页抽象接口，定义了一些缓存数据页的基本操作供具体实现类进行实现
public interface Page {
    //给当前缓存数据页进行加解锁操作
    void lock();
    void unlock();
    //将当前数据页的缓存数据释放掉
    void release();
    //设置当前数据页为脏页面
    void setDirty(boolean dirty);
    //判断当前数据页是否为脏页面
    boolean isDirty();
    //得到当前数据页的页号
    int getPageNumber();
    //得到当前数据页的实际缓存数据
    byte[] getData();
}
