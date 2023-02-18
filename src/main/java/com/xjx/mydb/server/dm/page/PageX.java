package com.xjx.mydb.server.dm.page;

import com.xjx.mydb.server.dm.pageCache.PageCache;
import com.xjx.mydb.server.utils.Parser;
import java.util.Arrays;

/**
 * @Author: Xjx
 * @Create: 2022/12/26 - 10:19
 * 管理数据库文件中除第一页之外的所有其他普通页
 * 普通页面以一个 2 字节无符号数起始，表示这一页的空闲位置的偏移。剩下的部分都是实际存储的数据。
 */
public class PageX {
    //空闲空间偏移量的起始地址
    private static final short OF_FREE = 0;
    //空闲空间偏移量大小为两个字节
    private static final short OF_DATA = 2;
    //最大空闲空间大小
    public static final int MAX_FREE_SPACE = PageCache.Page_SIZE - OF_DATA;

    //操作数据页时模拟出一个数据页
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.Page_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    //设置当前数据页的空闲空间偏移量
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    //获取一个数据页的当前空闲空间偏移量FSO
    public static short getFSO(Page page) {
        return getFSO(page.getData());
    }

    //一个数据页的实际数据存储在其data中
    private static short getFSO(byte[] raw) {
        //获取其实际数据中起始两个字节的数据并转化为short类型返回，即空闲空间偏移量
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    //将raw插入page中， 返回插入位置
    public static short insert(Page page, byte[] raw) {
        //新增操作，设置该缓存数据页为脏页面，缓存释放时需要写回
        page.setDirty(true);
        //获取当前页的空闲空间偏移量
        short offset = getFSO(page.getData());
        //将新插入的数据raw根据获得的偏移量复制到页实际数据的空闲空间中
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        //插入完数据之后更新空闲空间偏移量
        setFSO(page.getData(), (short)(offset + raw.length));
        //返回新插入数据的地址
        return offset;
    }

    //获取页面的空闲空间大小
    public static int getFreeSpace(Page page) {
        return PageCache.Page_SIZE - (int)getFSO(page.getData());
    }

    //此方法实现覆盖插入数据操作：将raw插入page中的offset位置，并将page的offset设置为较大的offset即空闲空间偏移量
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        //修改操作，设置该缓存数据页为脏页面，缓存释放时需要写回
        page.setDirty(true);
        //将传入的数据覆盖数据页中offset位置上的数据
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        //获取该数据页的空闲空间偏移量
        short rawFSO = getFSO(page.getData());
        //如果修改数据的地址大于原有空闲空间偏移量，则将FSO设置为修改数据之后的地址
        if(rawFSO < offset + raw.length) {
            setFSO(page.getData(), (short)(offset + raw.length));
        }
    }

    //此方法实现修改数据操作：将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }
}
