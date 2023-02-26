package com.xjx.mydb.server.vm;

import com.xjx.mydb.server.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Xjx
 * @Create: 2023/1/13 - 10:18
 */
public class Transaction {
    public long xid;
    //事务的隔离级别：0为读已提交1为可重复读
    public int level;
    //创建该事务时的其他事务是否活跃的快照
    public Map<Long, Boolean> snaphot;
    public Exception err;
    public boolean autoAborted;

    //创建新事务，用于保存快照数据的。但是如果是读已提交则无需保存快照数据
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            t.snaphot = new HashMap<>();
            for(long x : active.keySet()) {
                t.snaphot.put(x, true);
            }
        }
        return t;
    }

    //判断一个事务是否处在某个事务的快照数据中
    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snaphot.containsKey(xid);
    }
}
