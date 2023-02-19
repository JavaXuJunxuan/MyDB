package com.xjx.mydb.server.dm.pageCache;

import com.xjx.mydb.server.dm.page.Page;
import com.xjx.mydb.server.utils.Panic;
import com.xjx.mydb.server.utils.RandomUtil;
import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2022/12/27 - 14:04
 */
public class PageCacheTest {
    static Random random = new SecureRandom();

    //测试页缓存数据是否可用
    @Test
    public void testPageCache() throws Exception {
        //创建出一个数据库文件
        PageCache pc = PageCache.create("D:\\JavaProject\\MyDB\\tmp\\pcacher_simple_test0", PageCache.PAGE_SIZE * 50);
        //创建一百页缓存数据页，测试创建数据缓存页功能是否可用
        for (int i = 0; i < 100; i++) {
            byte[] tmp = new byte[PageCache.PAGE_SIZE];
            tmp[0] = (byte)i;
            int pgno = pc.newPage(tmp);
            //getPage方法从缓存页对象中取缓存如果取不到就去数据库文件中加载
            Page pg = pc.getPage(pgno);
            //将新创建的缓存数据页设置为脏页面，作用是因为我们现在操作的是内存中的数据缓存页对象，只有设置为
            //脏页面之后释放缓存才会将这个缓存数据写回数据库文件中
            pg.setDirty(true);
            pg.release();
        }
        pc.close();
        //测试缓存数据页写回功能是否正常
        pc = PageCache.open("D:\\JavaProject\\MyDB\\tmp\\pcacher_simple_test0", PageCache.PAGE_SIZE * 50);
        for(int i = 1; i <= 100; i++) {
            Page pg = pc.getPage(i);
            assert pg.getData()[0] == (byte)i - 1;
            //测试成功释放掉对应缓存
            pg.release();
        }
        pc.close();
        assert new File("D:\\JavaProject\\MyDB\\tmp\\pcacher_simple_test0.db").delete();
    }

    //测试高并发环境下缓存是否可用
    private PageCache pc1;
    private CountDownLatch cdl1;
    private AtomicInteger noPages1;
    @Test
    public void testPageCacheMultiSimple() throws Exception {
        pc1 = PageCache.create("D:\\JavaProject\\MyDB\\tmp\\pcacher_simple_test1", PageCache.PAGE_SIZE * 50);
        cdl1 = new CountDownLatch(200);
        noPages1 = new AtomicInteger(0);
        for(int i = 0; i < 200; i++) {
            int id = i;
            Runnable r = () -> worker1(1);
            new Thread(r).start();
        }
        cdl1.await();
        assert new File("D:\\JavaProject\\MyDB\\tmp\\pcacher_simple_test1.db").delete();
    }
    private void worker1(int id) {
        for(int i = 0; i < 80; i++) {
            //模拟实际生产环境中对缓存的读写操作
            int op = Math.abs(random.nextInt() % 20);
            //写缓存操作
            if(op == 0) {
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                int pgno = pc1.newPage(data);
                Page pg = null;
                try {
                    pg = pc1.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                noPages1.incrementAndGet();
                pg.release();
            } else if(op < 20) {
                //读缓存操作
                int mod = noPages1.intValue();
                if(mod == 0) {
                    continue;
                }
                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page pg = null;
                try {
                    pg = pc1.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                pg.release();
            }
        }
        cdl1.countDown();
    }

    private PageCache pc2, mpc;
    private CountDownLatch cdl2;
    private AtomicInteger noPages2;
    private Lock lockNew;
    @Test
    public void testPageCacheMulti() throws InterruptedException {
        pc2 = PageCache.create("D:\\JavaProject\\MyDB\\tmp\\pcacher_multi_test", PageCache.PAGE_SIZE * 10);
        mpc = new MockPageCache();
        lockNew = new ReentrantLock();
        cdl2 = new CountDownLatch(30);
        noPages2 = new AtomicInteger(0);

        for(int i = 0; i < 30; i++) {
            int id = i;
            Runnable r = () -> worker2(id);
            new Thread(r).run();
        }
        cdl2.await();
        assert new File("D:\\JavaProject\\MyDB\\tmp\\pcacher_multi_test.db").delete();
    }
    private void worker2(int id) {
        for(int i = 0; i < 1000; i++) {
            int op = Math.abs(random.nextInt() % 20);
            if(op == 0) {
                //new page
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                lockNew.lock();
                int pgno = pc2.newPage(data);
                int mpgno = mpc.newPage(data);
                assert pgno == mpgno;
                lockNew.unlock();
                noPages2.incrementAndGet();
            } else if(op < 10) {
                //check
                int mod = noPages2.intValue();
                if(mod == 0) continue;
                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page page = null, mpg = null;
                try {
                    page = pc2.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    mpg = mpc.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                page.lock();
                assert Arrays.equals(mpg.getData(), page.getData());
                page.unlock();
                page.release();
            } else {
                //update
                int mod = noPages2.intValue();
                if(mod == 0) continue;
                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page page = null, mpg = null;
                try {
                    page = pc2.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    mpg = mpc.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                byte[] newData = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                page.lock();
                mpg.setDirty(true);
                for(int j = 0; j < PageCache.PAGE_SIZE; j++) {
                    mpg.getData()[j] = newData[j];
                }
                page.setDirty(true);
                for(int j = 0; j < PageCache.PAGE_SIZE; j++) {
                    page.getData()[j] = newData[j];
                }
                page.unlock();
                page.release();
            }
        }
        cdl2.countDown();
    }
}
