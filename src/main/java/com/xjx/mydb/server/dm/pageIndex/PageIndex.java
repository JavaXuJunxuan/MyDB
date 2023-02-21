package com.xjx.mydb.server.dm.pageIndex;

import com.xjx.mydb.server.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2023/1/6 - 14:05
 * 页面索引类：用于缓存每个缓存数据页的空闲空间，帮助Insert操作更快速查找一个合适空间的页面
 */
public class PageIndex {
    //每一个页面的空间分为40个区间
    private static final int INTERVALS_NO = 40;
    //根据页面大小和区间总数计算单个区间的内存大小，同时也是空闲空间的单位大小
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    //使用一个类似于哈希表的形式即一个集合数组存放所有页面的空闲大小
    //数组中每一个集合都存放着空闲空间>=该下标位置*THRESHOLD的页面
    //数组大小为41，下标的含义就是区间号,区间号从1开始。比如1代表所有空闲空间能存储一个THRESHOLD大小的页面
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex(){
        lock = new ReentrantLock();
        //实例化页面索引中存储页面索引位置的数组
        lists = new List[INTERVALS_NO + 1];
        for(int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    //向页面索引中存储新页面，根据其空闲空间大小放入对应数组下标的集合中
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            //这里判断其空闲空间大小时是向下取整，因为空闲空间是以THRESHOLD为单位的
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    //根据所需空闲空间大小即spaceSize算出其区间号，然后从对应数组下标的集合中取出页面即可
    public PageInfo select (int spaceSize) {
        lock.lock();
        try {
            //计算能存储spaceSize大小的区间号时需要向上取整，因为可以页面内存只能大不能小
            int number = (spaceSize + THRESHOLD - 1) / THRESHOLD;
            //如果取整之后的number大于最大空闲内存能存储的大小，那么该数据无法存储在页面内
            while (number <= INTERVALS_NO) {
                if(lists[number].size() == 0){
                    number++;
                    continue;
                }
                //找到可以存储的页面后取出这个页面，意思是同一个页面是不允许并发写的。该页面用完后需重新插入页面索引
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
