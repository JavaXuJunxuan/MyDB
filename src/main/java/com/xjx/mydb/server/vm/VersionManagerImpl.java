package com.xjx.mydb.server.vm;

import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.common.AbstractCache;
import com.xjx.mydb.server.dm.DataManager;
import com.xjx.mydb.server.tm.TransactionManager;
import com.xjx.mydb.server.tm.TransactionManagerImpl;
import com.xjx.mydb.server.utils.Panic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2023/1/20 - 16:28
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {
    TransactionManager tm;
    DataManager dm;
    //当前VM中所有活跃的事务都会放在这个集合中
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }


    //读取一个Entry，需要判断可见性
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        //首先从当前VM的事务表中根据xid取出这个要读记录的事务
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        //如果这个事务执行出错，则抛出这个异常
        if(t.err != null) {
            throw t.err;
        }
        //获取记录，通过缓存框架AbstractCache获取，key即为记录的uid
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        //获取记录之后通过TM判断这个记录是否对当前事务可见，可见直接返回不可见则不返回
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            //最后记得取出的记录要及时释放掉，因为默认都放在缓存中了
            entry.release();
        }
    }

    //插入一条新记录
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        //判断一下当前事务是否出过错，有错则不执行
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }
        //将插入数据包装成记录，然后插入到对应的数据项中
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    //根据uid和xid删除这个的记录,要判断当前这个记录对事务是否可见
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            //因为VM实现了缓存框架，所以从那里获取记录
            entry = super.get(uid);
        } catch (Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            //可见性判断
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            //获取要删除的资源的锁
            Lock l = null;
            try {
                //涉及到删除操作需要加锁，为了死锁检测所以需要记录当前uid已经被xid占有了
                l = lt.add(xid, uid);
            } catch (Exception e) {
                //执行到这里表示出现了死锁
                t.err = Error.ConcurrentUpdateException;
                //死锁要自动回滚
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }
            //修改记录的删除事务id
            //如果已经被删除了且是当前事务删除则无需重复删除
            if(entry.getXmax() == xid) {
                return false;
            }
            //发生了版本跳跃要自动回滚
            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            //都没有问题才会进行删除即修改一下记录的标志即可
            entry.setXmax(xid);
            return true;
        } finally {
            entry.release();
        }
    }

    //开启一个新事务
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            //通过事务管理器TM开启一个新事务，但是它只维护事务数和状态返回的是事务xid
            long xid = tm.begin();
            //我们根据新建事务id创建一个事务对象出来，定义隔离级别和它属于VM下的活跃事务
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            //将事务加入活跃事务表中，用于检查和快照使用
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    //提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，并修改 TM 状态：
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        //通过当前VM的活跃事务表取出xid对应事务
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch (NullPointerException e) {
            Panic.panic(e);
        }
        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();
        //提交这个事务，去锁表中释放这个事务占有的锁
        lt.remove(xid);
        //tm中将事务状态改为提交
        tm.commit(xid);
    }

    //abort事务的方法则有两种，手动和自动。手动指的是调用下面这个abort() 方法
    //而自动则是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚：
    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        //不是自动则表示当前事务被主观abort即没有用了那么需要从VM中删除，如果自动删除可能有其他事务依赖此事务
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();
        //自动则直接退出
        if(t.autoAborted) return;
        //执行到这里说明手动abort所以锁表中将其删除事务管理器将其设置为abort
        lt.remove(xid);
        tm.abort(xid);
    }

    //释放掉这个记录的缓存
    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    //取出uid对应的记录
    @Override
    protected Entry getForCache(long uid) throws Exception {
        //这里加载的记录是从数据项那里加工过来的，实际访问记录的时候访问的是数据项的data
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.release();
    }
}
