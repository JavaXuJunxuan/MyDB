package com.xjx.mydb.server.common;

import com.xjx.mydb.common.Error;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2022/12/22 - 14:29
 * AbstractCache 实现了一个引用计数策略的缓存框架，管理缓存用的。泛型类，泛型是缓存的数据类型
 */
public abstract class AbstractCache<T> {
    // 实际缓存的数据，key为磁盘上实际数据的地址，value为缓存真实数据
    private HashMap<Long, T> cache;
    // 具体缓存数据的引用个数
    private HashMap<Long, Integer> references;
    // 记录哪些资源正在从数据源中获取。因为从数据源获取资源是一个相对费时的操作
    private HashMap<Long, Boolean> getting;
    // 缓存的最大缓存大小
    private int maxResource;
    // 缓存中缓存的个数
    private int count = 0;
    private Lock lock;

    public AbstractCache(int maxResource){
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }
    protected synchronized T get(long key) throws Exception {
        //通过 get() 方法获取资源时，首先进入一个死循环，来无限尝试从缓存里获取
        while (true) {
            //获取资源时为了多线程安全
//            lock.lock();
            //请求的资源是否正在被其他线程获取
            if(getting.containsKey(key)){
                //放弃锁并且睡一秒然后再次进入循环
//                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            //执行到此处表示没有线程正在获取要获取的缓存数据
            if(cache.containsKey(key)){
                //进入if中表示资源在缓存中，直接返回
                T obj = cache.get(key);
                //获取到缓存资源之后要将这个缓存的引用次数+1
                references.put(key, references.get(key) + 1);
//                lock.unlock();
                return obj;
            }
            //执行到此表示缓存中没有要获取的资源
            //判断缓存是否已经存满，如果存满就报错，因为我们是引用计数实现的缓存，是手动释放缓存数据
            if(maxResource > 0 && count == maxResource){
//                lock.unlock();
                throw Error.CacheFullException;
            }
            //执行到此处表示缓存无要获取的数据且缓存未满，此时尝试从数据源获取该资源
            count++;
            //修改这个数据的获取信息，表示此资源正在被某一个线程获取。
            getting.put(key, true);
//            lock.unlock();
            //退出循环，去数据源中获取资源并写入缓存
            break;
        }
        T obj = null;
        try {
            //从数据源获取资源，成功失败都要把从 getting 中删除 key
            obj = getForCache(key);
        } catch (Exception e){
            //获取失败时要回滚之前的缓存大小，此时要保证线程安全避免多减
//            lock.lock();
            count--;
            getting.remove(key);
//            lock.unlock();
            throw e;
        }
//        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
//        lock.unlock();
        return obj;
    }

    /**
     * 根据key即数据源上数据地址强行释放一个其对应的缓存数据
     */
    protected void release(long key) {
        lock.lock();
        try {
            //更新缓存数据的引用次数
            int ref = references.get(key) - 1;
            //一旦没有引用指向这个缓存，就需要驱逐这个缓存
            if(ref == 0){
                T obj = cache.get(key);
                //释放这个缓存所占的内存并将数据写入磁盘
                releaseForCache(obj);
                //引用表中除去这个缓存
                references.remove(key);
                //缓存表中删除这个缓存
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            //保证锁的释放，以免阻塞其他线程
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close(){
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys){
                T obj = cache.get(key);
                //写回数据
                releaseForCache(obj);
                //删除缓存中所有属性中引用的对象
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }
    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
