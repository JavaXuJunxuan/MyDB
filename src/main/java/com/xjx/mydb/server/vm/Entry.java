package com.xjx.mydb.server.vm;

import com.google.common.primitives.Bytes;
import com.xjx.mydb.server.common.SubArray;
import com.xjx.mydb.server.dm.dataItem.DataItem;
import com.xjx.mydb.server.utils.Parser;

import java.time.DateTimeException;
import java.util.Arrays;

/**
 * @Author: Xjx
 * @Create: 2023/1/12 - 16:37
 */
public class Entry {
    //创建这条记录的事务编号
    private static final int OF_XMIN = 0;
    //删除这条记录的事务编号
    private static final int OF_XMAX = OF_XMIN + 8;
    //这条记录持有的数据的起始地址
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    //记录保存在DataItem中，所以保存一个其引用
    private DataItem dataItem;
    private VersionManager vm;

    //根据传入的uid和数据项以及版本管理器创建一个新的记录
    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    //根据一个uid和一个版本管理器加载一条记录，记录存在数据项中，所以最后会调用dm从缓存中取数据项
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    //根据传入的数据包装成一个记录行的格式
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    //将记录释放掉
    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    //
    public void remove() {
        dataItem.release();
    }

    //以拷贝的形式返回数据内容
    public byte[] data() {
        dataItem.rLock();
        try {
            //先获取数据项中的数据
            SubArray sa = dataItem.data();
            //生成一个与数据等大的字节数组
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            //将数据拷贝到这个数组中然后返回
            System.arraycopy(sa.raw, sa.start + OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnlock();
        }
    }

    //得到创建这个记录的事务id
    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMIN, sa.start + OF_XMAX));
        } finally {
            dataItem.rUnlock();
        }
    }

    //得到删除这个记录的事务id
    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMAX, sa.start + OF_DATA));
        } finally {
            dataItem.rUnlock();
        }
    }

    //设置删除这个记录的事务id
    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start + OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    public long getUid() {
        return uid;
    }
}
