package com.xjx.mydb.server.vm;

import com.xjx.mydb.server.dm.DataManager;
import com.xjx.mydb.server.tm.TransactionManager;

/**
 * @Author: Xjx
 * @Create: 2023/1/20 - 10:49
 */
public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
