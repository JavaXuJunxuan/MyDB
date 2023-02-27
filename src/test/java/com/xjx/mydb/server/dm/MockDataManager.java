package com.xjx.mydb.server.dm;

import com.xjx.mydb.server.common.SubArray;
import com.xjx.mydb.server.dm.dataItem.DataItem;
import com.xjx.mydb.server.dm.dataItem.MockDataItem;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2023/1/15 - 14:20
 */
public class MockDataManager implements DataManager {
    private Map<Long, DataItem> cache;
    private Lock lock;

    public static MockDataManager newMockDataManager() {
        MockDataManager dm = new MockDataManager();
        dm.cache = new HashMap<>();
        dm.lock = new ReentrantLock();
        return dm;
    }

    @Override
    public DataItem read(long uid) throws Exception {
        lock.lock();
        try {
            return cache.get(uid);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        try {
            long uid = 0;
            while(true) {
                uid = Math.abs(new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE));
                if(uid == 0) continue;
                if(cache.containsKey(uid)) continue;
                break;
            }
            DataItem di = MockDataItem.newMockDataItem(uid, new SubArray(data, 0, data.length));
            cache.put(uid, di);
            return uid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {}
}
