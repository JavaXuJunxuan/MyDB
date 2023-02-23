package com.xjx.mydb.server.tm;

import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2022/12/26 - 17:42
 */
public class TransactionManagerTest {
    static Random random = new SecureRandom();

    private int transCount = 0;
    private int noWorkers = 50;
    private int noWorks = 3000;
    private Lock lock = new ReentrantLock();
    private TransactionManager tm;
    private Map<Long, Byte> transMap;
    private CountDownLatch cdl;

    @Test
    public void testMultiThread() {
        tm = TransactionManager.create("D:\\JavaProject\\MyDB\\tmp\\trans_test");
        //统计一共执行过多少个事务
        transMap = new ConcurrentHashMap<>();
        //多线程环境（50个线程）下并发处理
        cdl = new CountDownLatch(noWorkers);
        for(int i = 0; i < noWorkers; i++){
            Runnable r = () -> worker();
            new Thread(r).start();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assert new File("D:\\JavaProject\\MyDB\\tmp\\trans_test.xid").delete();
    }

    private void worker() {
        //事务是否正在进行的标志
        boolean inTrans = false;
        //每一次循环中事务的ID,从1开始递增，0为超级事务。
        long transXID = 0;
        for(int i = 0; i < noWorks; i++){
            //这里为了模拟生产环境中事务的出现比率，所以给每一个事务随机取一个0-5的数
            int op = Math.abs(random.nextInt(6));
            //如果随机出的是0，那么表示此时要执行的操作需要进行事务处理
            if(op == 0){
                //此时为了保证操作多线程安全所以需要加锁
                lock.lock();
                //如果当前没有事务运行则修改事务运行标志，表示创建一个新事务执行此次数据操作
                if(inTrans == false){
                    long xid = tm.begin();
                    transMap.put(xid, (byte)0);
                    transCount++;
                    transXID = xid;
                    inTrans = true;
                }else {
                    //进行到此处表示当前数据库中有事务正在运行，则先处理上一个事务
                    //这里为了模拟随机取出一个1/2表示对此事务进行提交或者回滚处理
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch (status){
                        case 1:
                            tm.commit(transXID);
                            break;
                        case 2:
                            tm.abort(transXID);
                            break;
                    }
                    //这里把处理完的事务统计一下，然后修改事务运行标志
                    transMap.put(transXID, (byte)status);
                    inTrans = false;
                }
                lock.unlock();
            } else {
                //执行到这里表示不需要执行事务，检验过往事务是否存在异常
                lock.lock();
                if(transCount > 0){
                    long xid = (long)((random.nextInt(Integer.MAX_VALUE) % transCount) + 1);
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status){
                        case 0:
                            ok = tm.isActive(xid);
                            break;
                        case 1:
                            ok = tm.isCommitted(xid);
                            break;
                        case 2:
                            ok = tm.isAborted(xid);
                            break;
                    }
                    assert ok;
                }
                lock.unlock();
            }
        }
        cdl.countDown();
    }
}
