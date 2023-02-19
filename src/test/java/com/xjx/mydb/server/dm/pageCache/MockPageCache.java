package com.xjx.mydb.server.dm.pageCache;

import com.xjx.mydb.server.dm.page.MockPage;
import com.xjx.mydb.server.dm.page.Page;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2022/12/27 - 10:29
 */
public class MockPageCache implements PageCache {
    //实现一个缓存空间用于存储缓存数据，用作测试
    private Map<Integer, MockPage> cache = new HashMap<>();
    private Lock lock = new ReentrantLock();
    //数据页的总页号
    private AtomicInteger noPages = new AtomicInteger(0);

    //为传入的需要写入缓存的数据新增一个数据页
    @Override
    public int newPage(byte[] initData) {
        lock.lock();
        try {
            //得到最大数据页号
            int pgno = noPages.incrementAndGet();
            //新建数据页
            MockPage page = MockPage.newMockPage(pgno, initData);
            //放入临时创建的缓存空间
            cache.put(pgno, page);
            return pgno;
        } finally {
            lock.unlock();
        }
    }

    //根据页号得到缓存数据
    @Override
    public Page getPage(int pgno) throws Exception {
        lock.lock();
        try {
            return cache.get(pgno);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {}

    @Override
    public void release(Page page) {}

    @Override
    public void truncateByBgno(int maxPgno) {}

    @Override
    public int getPageNumber() {
        return noPages.intValue();
    }

    @Override
    public void flushPage(Page pg) {}
}
