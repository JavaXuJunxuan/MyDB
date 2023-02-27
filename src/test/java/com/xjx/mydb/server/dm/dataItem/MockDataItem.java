package com.xjx.mydb.server.dm.dataItem;

import com.xjx.mydb.server.common.SubArray;
import com.xjx.mydb.server.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author: Xjx
 * @Create: 2023/1/13 - 10:13
 */
public class MockDataItem implements DataItem {

    private SubArray data;
    private byte[] oldData;
    private long uid;
    private Lock rLock;
    private Lock wLock;

    public static MockDataItem newMockDataItem(long uid, SubArray data) {
        MockDataItem di = new MockDataItem();
        di.data = data;
        di.oldData = new byte[data.end - data.start];
        di.uid = uid;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        di.rLock = lock.readLock();
        di.wLock = lock.writeLock();
        return di;
    }

    @Override
    public SubArray data() {
        return data;
    }

    @Override
    public void before() {
        wLock.lock();
        System.arraycopy(data.raw, data.start, oldData, 0, oldData.length);
    }

    @Override
    public void unBefore() {
        wLock.lock();
        System.arraycopy(oldData, 0, data.raw, data.start, oldData.length);
    }

    @Override
    public void after(long xid) {
        wLock.unlock();
    }

    @Override
    public void release() {}

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
        return null;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldData;
    }

    @Override
    public SubArray getRaw() {
        return data;
    }
}
