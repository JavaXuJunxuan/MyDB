package com.xjx.mydb.server.dm.dataItem;

import com.xjx.mydb.server.common.SubArray;
import com.xjx.mydb.server.dm.DataManagerImpl;
import com.xjx.mydb.server.dm.logger.Logger;
import com.xjx.mydb.server.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author: Xjx
 * @Create: 2023/1/7 - 17:48
 */
public class DataItemImpl implements DataItem {
    //数据的有效位存在数据文件的第一个字节处
    static final int OF_VALID = 0;
    //存储数据的长度存在数据文件的第二个字节处，长度为两个字节
    static final int OF_SIZE = 1;
    //实际数据的存储起始地址为数据文件的第四字节处
    static final int OF_DATA = 3;

    //数据对象的数据与数据页中的内存数据共享同一段内存
    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    //一个引用指向了DM，通过DM来管理数据项，比如缓存DataItem和释放DataItem
    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.rLock = lock.readLock();
        this.wLock = lock.writeLock();
    }
    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte)0;
    }

    @Override
    public SubArray data() {
        //返回数据项中保存的实际数据，这里需要操作数据项存放的共享数组
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    //对数据修改的前置操作，需要先加写锁，保证并发安全然后设置修改设计的页面为脏页面，
    //方便缓存数据页释放时将数据写回磁盘。然后保存旧数据
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    //撤回之前的修改操作
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();

    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnlock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
