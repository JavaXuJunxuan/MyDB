package com.xjx.mydb.server.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2022/12/27 - 8:58
 */
public class MockPage implements Page {
    //这里是测试缓存数据页，无法从数据库读取数据，所以我们手动输入测试数据
    //表示数据页的页号
    private int pgno;
    //数据页的实际数据
    private byte[] data;
    private Lock lock = new ReentrantLock();

    public static MockPage newMockPage(int pgno, byte[] data) {
        MockPage mp = new MockPage();
        mp.pgno = pgno;
        mp.data = data;
        return mp;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {}

    @Override
    public void setDirty(boolean dirty) {}

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public int getPageNumber() {
        return pgno;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
