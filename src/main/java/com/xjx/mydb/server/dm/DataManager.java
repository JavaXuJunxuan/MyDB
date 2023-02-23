package com.xjx.mydb.server.dm;

import com.xjx.mydb.server.dm.dataItem.DataItem;
import com.xjx.mydb.server.dm.logger.Logger;
import com.xjx.mydb.server.dm.page.PageOne;
import com.xjx.mydb.server.dm.pageCache.PageCache;
import com.xjx.mydb.server.tm.TransactionManager;

/**
 * @Author: Xjx
 * @Create: 2023/1/9 - 10:42
 */
public interface DataManager {
    //DM 层提供了三个功能供上层使用，分别是读、插入和修改，修改是通过读出的 DataItem 实现的。
    //因此我们只需要实现读和插入方法即可
    //根据uid读取数据，uid=数据页号+数据在页中偏移量
    DataItem read(long uid) throws Exception;
    //插入数据涉及修改，需要传入事务id和插入的数据
    long insert(long xid, byte[] data) throws Exception;
    void close();

    //从空文件创建 DataManager 首先需要对第一页进行初始化。因为数据库第一页不保存数据，只是做启动检查的
    public static DataManager create(String path, long mem, TransactionManager tm) throws Exception {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    //读取数据库文件并生成缓存数据与日志，返回数据操作对象
    //且需要对第一页进行校验，来判断是否需要执行恢复流程。并重新对第一页生成随机字节。
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        //校验完毕之后把第一页数据修改成异常退出的格式，如果正常退出我们会将其改为正常退出格式
        PageOne.setVcClose(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }
}
