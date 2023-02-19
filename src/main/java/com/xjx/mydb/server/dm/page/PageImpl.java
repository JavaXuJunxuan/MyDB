package com.xjx.mydb.server.dm.page;

import com.xjx.mydb.server.dm.pageCache.PageCache;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2022/12/23 - 10:02
 */
//这是缓存数据页接口的实现类，其实例是放在内存中。这个实现类定义了一些缓存数据页的参数信息，具体缓存数据放在缓存页对象pc中
public class PageImpl implements Page {
    //数据页的页号，从1开始
    private int pageNumber;
    //数据页中真正存储数据的地方即这个页实际包含的字节数据
    private byte[] data;
    //标记一个页是否为脏页面。缓存驱逐的时候，脏页面需要被写回磁盘.比如设计到更新插入操作时都需要设置数据页为脏页面
    private boolean dirty;
    private Lock lock;
    //指向当前缓存数据页的缓存数据，方便在拿到 Page 的引用时可以快速对这个页面的缓存进行释放操作。
    private PageCache pc;

    //缓存数据页号，页数据，指向缓存数据的对象
    public PageImpl(int pageNumber, byte[] data, PageCache pc){
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    //释放缓存数据页对应的缓存数据
    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    //得到缓存数据页中存储的真正数据
    @Override
    public byte[] getData() {
        return data;
    }
}


