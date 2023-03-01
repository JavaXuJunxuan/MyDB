package com.xjx.mydb.server.dm.pageCache;

import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.common.AbstractCache;
import com.xjx.mydb.server.dm.page.Page;
import com.xjx.mydb.server.dm.page.PageImpl;
import com.xjx.mydb.server.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2022/12/24 - 09:40
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache{
    //一个缓存数据区中最少的缓存数据页页数
    private static final int MEM_MIN_LEN = 10;
    //数据库文件的后缀
    public static final String DB_SUFFIX = ".db";
    //该文件指向的是磁盘上的数据库文件即数据文件不是缓存文件，因为缓存是存在内存中的
    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock fileLock;
    //AtomicInteger原子类来记录当前打开的数据库文件有多少页。这个数字在数据库文件被打开时就会被计算，并在新建页面时自增。
    private AtomicInteger pageNumbers;

    public PageCacheImpl(RandomAccessFile raf, FileChannel fc, int maxResource) {
        //执行父类AbstractCache的有参构造，设定最大缓存数据个数
        super(maxResource);
        //如果分配的数据缓存页小于最小的缓存页则退出
        if(maxResource < MEM_MIN_LEN) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = raf.length();
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        this.raf = raf;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        //根据读取文件的长度/每个数据页大小获取当前这个数据库文件一共多少页数据
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    //根据页号获取数据页，调用的是缓存框架AbstractCache的方法，如果缓存中有从缓存取，如果缓存没有去数据库取。
    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long) pgno);
    }

    //给当前缓存页新增的缓存数据创建新的缓存页，即新建的缓存数据在原有页中存不下了，需要新建数据页
    @Override
    public int newPage(byte[] initData) {
        //读取最新的数据页号并根据此页号创建新缓存数据页
        int pgno = pageNumbers.incrementAndGet();
        //这个page对象就是缓存数据页，放在JVM堆中即计算机内存中
        Page page = new PageImpl(pgno, initData, null);
        //需要把缓存数据刷回磁盘上的数据库文件中
        flush(page);
        return pgno;
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    //将参数传来的缓存数据页刷回数据库文件
    private void flush(Page page) {
        int pgno = page.getPageNumber();
        //缓存数据页所在偏移就是数据库文件中对应数据偏移量
        long offset = pageOffset(pgno);
        fileLock.lock();
        try {
            //得到缓存数据页中真正的缓存数据，并写回磁盘中的数据库文件
            ByteBuffer buffer = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buffer);
            fc.force(false);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        } finally {
            fileLock.unlock();
        }
    }

    private static long pageOffset(int pgno) {
        return (pgno - 1) * PAGE_SIZE;
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            raf.close();
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
    }

    /**
     * 根据pageNumber从数据库文件(通过NIO操作文件)中读取页数据，并包裹成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buffer.array(), this);
    }

    //释放某个缓存数据页的在内存中的缓存数据
    @Override
    protected void releaseForCache(Page page) {
        //如果是脏页面则要将缓存数据刷回磁盘在释放
        if(page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }
    }

    //根据传入的缓存数据页得到其页号，然后使用AbsractCache父类的release方法释放这个缓存数据页的缓存
    //但我们项目中使用的缓存框架是引用计数框架，所以释放缓存数据实际操作是减少这个缓存数据的被引用次数，当=0时才会真正释放掉缓存数据
    @Override
    public synchronized void release(Page page) {
        release((long)page.getPageNumber());
    }

    //根据传入的最大缓存数据页数对数据库文件进行截断，也就是说这个数据库文件最多存放maxPgno页缓存数据
    @Override
    public void truncateByBgno(int maxPgno) {
        //根据传入的最大页号算缓存最大内存容量
        long size = pageOffset(maxPgno + 1);
        try {
            raf.setLength(size);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        //设置数据库文件最大数据页数，至少为1页
        pageNumbers.set(maxPgno);
    }

    //获得当前数据库文件最大数据页号
    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }


}
