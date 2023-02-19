package com.xjx.mydb.server.dm.pageCache;

import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.dm.page.Page;
import com.xjx.mydb.server.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @Author: Xjx
 * @Create: 2022/12/24- 08:32
 */
//页缓存数据接口，指向缓存数据页中真正的缓存数据
public interface PageCache {
    //定义每个数据缓存页大小为16KB
    public static final int PAGE_SIZE = 1 << 14;

    //根据传入的字节数据创建一个新缓存数据页
    int newPage(byte[] initData);
    //根据传入的页号获取对应缓存数据页
    Page getPage(int pgno) throws Exception;
    //关闭数据页缓存，此时释放所有数据页缓存并写回
    void close();
    //根据传入的数据缓存页释放其缓存数据
    void  release(Page page);
    //根据最大数据页号截断数据页
    void truncateByBgno(int maxPgno);
    //获取该缓存数据的缓存数据页号
    int getPageNumber();
    //根据传入的数据页刷新缓存数据
    void flushPage(Page pg);

    //该方法用于根据传入的地址及内存大小创建出新的缓存数据页并返回缓存数据对象
    public static PageCacheImpl create(String path, long memory) {
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if(!file.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        if(!file.canRead() || !file.canWrite()){
            Panic.panic(Error.FileCannotRWExcepiton);
        }
        //获得对这个文件操作的IO对象
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory / PAGE_SIZE);
    }
    //根据传入的地址读取磁盘上的数据并写入缓存
    public static PageCacheImpl open(String path, long memory) {
        File file= new File(path + PageCacheImpl.DB_SUFFIX);
        if(!file.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if(!file.canRead() || !file.canWrite()){
            Panic.panic(Error.FileCannotRWExcepiton);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory / PAGE_SIZE);
    }
}
