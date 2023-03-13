package com.xjx.mydb.server.vm;

import com.google.common.collect.Lists;
import com.xjx.mydb.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2023/1/18 - 20:54
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    // 某个XID已经获得的资源的UID列表
    private Map<Long, List<Long>> x2u;
    // UID被某个XID持有
    private Map<Long, Long> u2x;
    // 正在等待UID的XID列表
    private Map<Long, List<Long>> wait;
    // 正在等待资源的XID的锁
    private Map<Long, Lock> waitLock;
    // XID正在等待的UID
    private Map<Long, Long> waitU;
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    //向某个xid事务中加入uid资源
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            //当前事务已经获得了uid的资源则无需等待
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            //如果这个uid资源未被占用，则将该xid放入占用表中做记录然后也无需等待
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                //同时还用把占有资源记录在xid持有资源的表中
                putIntoList(x2u, xid, uid);
                return null;
            }
            //执行到这里表示这个uid资源被人占用，记录一下表示xid正在等待uid资源
            waitU.put(xid, uid);
            //将某个xid正在等待的uid放入集合
            putIntoList(wait, xid, uid);
            //这里进入等待资源了，我们需要判断一下是否存在死锁
            if(hasDeadLock()) {
                //进入if表示有死锁
                //把刚才这个等待uid资源的xid删除掉，因为它会死锁
                waitU.remove(xid);
                //处于等待状态的xid事务表也要将其删除
                removeFromList(wait, uid, xid);
                //抛出死锁异常
                throw Error.DeadlockException;
            }
            //到这里表示没有死锁
            Lock l = new ReentrantLock();
            //进入等待资源状态，并且向等待资源锁表加入这个xid及其锁
            //返回这个上了锁的lock对象的目的就是阻塞这个要获取资源的xid，因为其他事务有这个资源了
            //那么它想获取这个资源首先得得到这个对象的锁（在占有这个资源的事务那里）
            //即2PL的阻塞，保证涉及并发修改时调度序列的串行化，避免数据不一致
            l.lock();
            waitLock.put(xid, l);
            return l;
        } finally {
            lock.unlock();
        }
    }

    //在一个事务 commit 或者 abort 时，就可以释放所有它持有的锁，并将自身从等待图中删除。
    public void remove(long xid) {
        lock.lock();
        try {
            //释放这个事务所占有的所有uid资源
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            //将其在执行时等待的uid也从等待表中删除
            waitU.remove(xid);
            //将其从占有uid资源的xid表中删除
            x2u. remove(xid);
            //将其从等待资源锁的xid表中删除（这个事务之前请求的资源被他人占用）
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    //从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        //将之前占有这个uid资源的xid从uid占有表中删除
        u2x.remove(uid);
        //取出等待这个uid资源的xid列表
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert l.size() > 0;
        //如果列表不为空且有元素表示有xid等待这个uid
        while (l.size() > 0) {
            //从列表中取出第一个等待这个uid资源的xid事务即公平锁
            long xid = l.remove(0);
            //如果这个xid没有在等待这个资源的锁表中则跳过
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                //直到找到一个等待锁表中的xid，然后占用这个uid和把其锁给这个xid并解锁，然后等待队列中移除这个xid
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }
        if(l.size() == 0) wait.remove(uid);
    }

    //给所有事务记录一个访问戳（相同事务中的操作访问戳相同），不同事务的访问戳不同，出现相同时则死锁。
    private Map<Long, Integer> xidStamp;
    //一个访问戳，用来判断是否成环即死锁的
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        //遍历所有获得过资源的事务
        for (long xid : x2u.keySet()){
            //如果事务的访问戳大于0表示访问过无需再访问
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            stamp++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    //对
    private boolean dfs(long xid) {
        //取出这个事务的访问戳
        Integer stp = xidStamp.get(xid);
        //有访问戳且=stamp表示之前访问过该节点则死锁
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);
        //看一下这个深搜的xid是否正在等待某个uid资源：没有等待则一定不会死锁
        Long uid = waitU.get(xid);
        if(uid == null) return false;
        //执行到这里表示当前xid事务在等待某个uid资源则判断一下这个uid资源是否被其他事务占有
        Long x = u2x.get(uid);
        //如果不为null表示有事务占有这个uid，此处正常一定不为null
        //因为前面add方法xid试图占有uid资源的时候就已经判断过这个uid资源被其他事务占有才会执行到这里进行死锁判断
        assert x != null;
        //找到这个占有uid资源的事务继续深搜
        return dfs(x);
    }

    //从某个列表中将uid0对应的列表中删除其值为uid1的对象
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        //根据传的key取出对应的列表
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }
    //将当前某个资源放在某个列表的uid0键对应的那个集合中
    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1){
        //判断集合中是否存放过这个key
        if(!listMap.containsKey(uid0)) {
            //没有存放过则会新建一个键值对
            listMap.put(uid0, new ArrayList<>());
        }
        //然后把元素存放进去
        listMap.get(uid0).add(0, uid1);
    }

    //判断某个列表中uid0键对应的那个集合中是否有uid1
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while (i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }
}
