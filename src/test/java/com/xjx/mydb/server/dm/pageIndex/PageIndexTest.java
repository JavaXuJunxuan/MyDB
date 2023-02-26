package com.xjx.mydb.server.dm.pageIndex;

import com.xjx.mydb.server.dm.pageCache.PageCache;
import org.junit.Test;

/**
 * @Author: Xjx
 * @Create: 2023/1/11 - 16:06
 */
public class PageIndexTest {
    //测试页面索引是否可以插入指定内存大小页面以及可否取出指定内存大小页面
    @Test
    public void testPageIndex() {
        PageIndex pIndex = new PageIndex();
        int threshold = PageCache.PAGE_SIZE / 20;
        for(int i = 0; i < 20; i++){
            pIndex.add(i, i * threshold);
            pIndex.add(i, i * threshold);
            pIndex.add(i, i * threshold);
        }
        for (int k = 0; k < 3; k++) {
            for(int i = 0; i < 19; i++) {
                PageInfo pi = pIndex.select(i * threshold);
                assert pi != null;
                assert pi.pgno == i + 1;
            }
        }
    }
}
