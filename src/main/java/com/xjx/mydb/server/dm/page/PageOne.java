package com.xjx.mydb.server.dm.page;

import com.xjx.mydb.server.dm.pageCache.PageCache;
import com.xjx.mydb.server.utils.RandomUtil;
import java.util.Arrays;

/**
 * @Author: Xjx
 * @Create: 2022/12/29 - 17:50
 * 特殊管理数据库文件的第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageOne {
    //数据库文件中启动检查的起始地址
    private static final int OF_VC = 100;
    //数据库文件中启动检查的数据长度
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        //数据库文件启动时首先模拟出第一页缓存数据页
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    //启动检查失败时调用此方法即上次数据库异常关闭，标记此数据页为脏数据并重新生成随机字节数组
    private static void setVcOpen(Page page){
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    //数据库启动时随机生成一个八字节长度的字节数组存储在第一页缓存数据页的100-107字节处
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    //异常关闭调用此方法，表示此页为脏数据
    public static void setVcClose(Page page) {
        page.setDirty(true);
        setVcClose(page.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    //因为Page是数据缓存页，它还包含了一些页相关信息，真正缓存数据都在data字节数组中
    public static boolean checkVc(Page page) {
        return checkVc(page.getData());
    }

    //启动检查比对第一页缓存数据中100 ~ 107 字节和108-115字节是否相同
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC),
                Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC));
    }

}
