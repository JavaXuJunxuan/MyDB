package com.xjx.mydb.server.dm;

import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.common.AbstractCache;
import com.xjx.mydb.server.dm.dataItem.DataItem;
import com.xjx.mydb.server.dm.dataItem.DataItemImpl;
import com.xjx.mydb.server.dm.logger.Logger;
import com.xjx.mydb.server.dm.page.Page;
import com.xjx.mydb.server.dm.page.PageOne;
import com.xjx.mydb.server.dm.page.PageX;
import com.xjx.mydb.server.dm.pageCache.PageCache;
import com.xjx.mydb.server.dm.pageIndex.PageIndex;
import com.xjx.mydb.server.dm.pageIndex.PageInfo;
import com.xjx.mydb.server.tm.TransactionManager;
import com.xjx.mydb.server.utils.Panic;
import com.xjx.mydb.server.utils.Types;

/**
 * @Author: Xjx
 * @Create: 2023/1/9 - 13:57
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {
    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    //根据一个处理过的数据项的uid解析为对应的页号+偏移量并得到该数据转移成数据项返回
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 32)) - 1);
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page page = pc.getPage(pgno);
        return DataItem.parseDataItem(page, offset, this);
    }

    //因为数据都是以页为单位处理的，所以释放数据项缓存释放其数据页缓存就可以了
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    //通过一个uid即页号+偏移量读取对应位置的数据，并返回这个数据对应的数据项
    @Override
    public DataItem read(long uid) throws Exception {
        //先从缓存中取取不到才去数据库
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    //插入操作，返回值是一个uid即页号+偏移量
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        //讲一个要插入的数据包装成数据库中数据格式
        byte[] raw = DataItem.wrapDataItemRaw(data);
        //如果要插入的数据比数据页最大空闲内存都大则报错
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        //为要插入的数据去页面索引中找到能容下这个数据的页面
        PageInfo pi = null;
        //一共查找5次，都找不到则说明目前数据库比较繁忙，没有空闲内存。
        for(int i = 0; i < 5; i++) {
            //找可存储的页面
            pi = pIndex.select(raw.length);
            if(pi != null){
                break;
            } else {
                //则不到则新建页面
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        //如果还是null则数据库繁忙错误
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }
        //执行到此处表示找到一个可以存储新插入数据的页面
        Page page = null;
        int freeSpace = 0;
        try {
            //通过找到的页面信息找到这个数据缓存页
            page = pc.getPage(pi.pgno);
            //插入操作之前需要先写日志，将此次操作的事务id和要插入的数据页以及要插入的数据传进去
            byte[] log = Recover.insertLog(xid, page, raw);
            logger.log(log);
            //插入操作返回其在页内偏移量
            short offset = PageX.insert(page, raw);
            //插入之后要及时释放缓存
            page.release();
            //返回这个插入数据的uid
            return Types.addressToUid(pi.pgno, offset);
        } finally {
            //将取出的page重新插入pIndex,因为页面索引中取得页面是从中删除了，用完需要加回去
            if(page != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(page));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    //关闭DM即代表着断开与数据库的连接了
    @Override
    public void close() {
        //关闭缓存
        super.close();
        logger.close();
        //关闭对数据库的操作时需要修改第一页的检查信息
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    //为xid生成update日志
    //更新操作也要生成日志，因为涉及到了修改
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    //释放这个数据项对象的缓存，即引用计数减-1或者删除并写回磁盘
    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    //创建数据库文件时初始化PageOne
    void initPageOne() throws Exception {
        int pgno = pc.newPage(PageOne.InitRaw());
        if(pgno != 1) {
            throw Error.DatabaseConnectFailedException;
        }
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        //将创建的数据库第一页写回磁盘
        pc.flushPage(pageOne);
    }

    //在打开已有文件时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    //第一次连接数据库时需要初始化pageIndex即读取数据库全部的数据并缓存进页面索引
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i++) {
            Page page = null;
            try {
                page = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(page.getPageNumber(), PageX.getFreeSpace(page));
            //每次读取数据之后因为都是把数据以对象形式放入内存即缓存中了，所以用完要记得释放
            page.release();
        }
    }
}
