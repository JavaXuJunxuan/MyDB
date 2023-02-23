package com.xjx.mydb.server.dm.dataItem;

import com.google.common.primitives.Bytes;
import com.xjx.mydb.server.common.SubArray;
import com.xjx.mydb.server.dm.DataManagerImpl;
import com.xjx.mydb.server.dm.page.Page;
import com.xjx.mydb.server.utils.Parser;
import com.xjx.mydb.server.utils.Types;

import java.util.Arrays;

/**
 * @Author: Xjx
 * @Create: 2023/1/7 - 10:30
 *  DM提供给上层模块的数据库数据的抽象，其实例就是数据库中的数据
 */
public interface DataItem {
    //用于存储返回数据的数组是数据共享的，不是利用拷贝实现的
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnlock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    //将读取到的字节数据包装成一个Java可以操作的数据项对象返回
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的offset处解析处dataitem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        //获取数据的长度
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        //数据实际长度=数据长度+头信息
        short length = (short)(size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
