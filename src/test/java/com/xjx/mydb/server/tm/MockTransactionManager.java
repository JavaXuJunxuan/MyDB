package com.xjx.mydb.server.tm;

/**
 * @Author: Xjx
 * @Create: 2022/12/26 - 17:32
 */
public class MockTransactionManager implements TransactionManager{
    @Override
    public long begin() {
        return 0;
    }

    @Override
    public void commit(long xid) {
    }

    @Override
    public void abort(long xid) {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isActive(long xid) {
        return false;
    }

    @Override
    public boolean isCommitted(long xid) {
        return false;
    }

    @Override
    public boolean isAborted(long xid) {
        return false;
    }
}
