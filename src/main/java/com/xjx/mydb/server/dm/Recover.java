package com.xjx.mydb.server.dm;

import com.google.common.primitives.Bytes;
import com.xjx.mydb.server.common.SubArray;
import com.xjx.mydb.server.dm.logger.Logger;
import com.xjx.mydb.server.dm.page.Page;
import com.xjx.mydb.server.dm.page.PageX;
import com.xjx.mydb.server.dm.pageCache.PageCache;
import com.xjx.mydb.server.tm.TransactionManager;
import com.xjx.mydb.server.utils.Panic;
import com.xjx.mydb.server.utils.Parser;

import java.util.*;

/**
 * @Author: Xjx
 * @Create: 2023/1/4 - 9:31
 */
public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;
    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        //事务id
        long xid;
        //操作的页号
        int pgno;
        //操作数据的偏移量
        short offset;
        //新增的数据
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        //日志对象对应的日志数据在日志文件中的偏移量
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

     //通过日志文件恢复数据库数据
     public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        //将偏移量改为第一个日志的偏移量
        lg.rewind();
        //遍历日志文件中的所有日志，维护一个日志所操作的缓存数据页的最大页数
        int maxPgno = 0;
        while (true) {
            //因为DM的日志遍历方式是迭代，不断获取下一个日志文件
            byte[] log = lg.next();
            //如果没有下一个日志了就表示当前日志文件所有可恢复的日志都以恢复完毕
            if(log == null) break;
            //
            int pgno;
            //日志只有两种：1是插入2是更新。判断类型之后将日志解析为对应日志格式
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        //然后根据所操作的最大数据页号获取其实际地址然后根据这个最大地址截断日志文件
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to" + maxPgno + "pages.");
        //开始恢复数据库，至于是重做还是撤销由恢复方法自行判断（tm.isActive(xid)判断对应事务状态）
        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");
        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");
        System.out.println("Recovery Over");
     }

    //恢复数据库操作中有两步，这是第一步重做所有已完成项目
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        //将偏移量转到日志文件的实际日志数据起始地址
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    //恢复数据库操作第二步：撤销所有未完成事务
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        //首先统计出所有的日志，以事务进行区分
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while (true) {
            //获取日志数据
            byte[] log = lg.next();
            if(log == null) break;
            //判断日志数据的类型
            if(isInsertLog(log)) {
                //将日志数据转化为日志对象
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                //判断执行此操作的事务是否是未执行完的事务
                if(tm.isActive(xid)) {
                    if (logCache.containsKey(xid)) {
                        //给之前没统计过的事务创建一个新集合
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }
        //对所有active log进行倒序undo
        //将每一个集合的事务单独取出执行
        for(Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            //执行当前事务的所有日志
            List<byte[]> logs = entry.getValue();
            //倒序执行日志
            for(int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                //重做和撤销只需要更改一个标志位即可
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    //传入日志数据判断是否为插入操作，这里log是实际日志数据不包含日志头信息的
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    //下面是更新操作的相关API
    //更新操作日志数据格式：[LogType][XID][UID][OldRaw][NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    //用于创建更新日志，参数是事务id和对应的数据项
    public static byte[] updateLog(long xid, DataItem di) {
        //将日志类型标志位该给更新操作的标志
        byte[] logType = {LOG_TYPE_UPDATE};
        //将传入的事务id处理为byte字节数组
        byte[] xidRaw = Parser.long2Byte(xid);
        //将数据项中的唯一标识uuid处理为字节数组
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        //获取此次更新操作的原数据
        byte[] oldRaw = di.getOldRaw();
        //获取此次操作数据的内存数组
        SubArray raw = di.getRaw();
        //通过数据的内存数组获取新数据
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        //将以上日志数据合并为一个完整的日志
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    //将一个日志数据解析为更新日志对象
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        //获取日志数据中的xid并解析为long类型赋值给日志对象
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        //获取日志文件对应的偏移量
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        //将更新日志中的新老值分别赋值给更新日志对象
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return li;
    }

    //用于恢复数据库执行日志文件中保存的更新日志信息
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        //通过标志位判断是重写操作还是撤销操作
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page page = null;
        try {
            //通过页缓存区根据页号获取对应缓存数据页对象
            page = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            //根据更新日志获取的新值更新缓存数据
            PageX.recoverUpdate(page, raw, offset);
        } finally {
            //执行完这一更新操作之后把这个缓存对象释放，因为在上面更新这个缓存数据页的缓存数据之后
            //这个页已经是脏页面，如果没有其他页面引用这个缓存数据，那么就会把这次更新之后的数据刷回数据库
            page.release();
        }
    }
    //下面是插入操作的相关API
    //插入操作日志数据格式：[LogType][XID][Pgno][Offset][Raw]
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    //根据事务id，缓存数据页，新插入数据生成一个插入日志数据（这个数据可以直接存储在日志文件中）
    public static byte[] insertLog(long xid, Page page, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(page.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(page));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    //根据传入的插入日志数据解析成一个插入日志对象
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        return li;
    }

    //用于恢复数据库时执行日志文件中保存的插入日志信息
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        //将日志文件中对应的插入操作日志转化为插入日志对象
        InsertLogInfo li = parseInsertLog(log);
        Page page = null;
        try {
            //获取该插入日志所插入数据的位置
            page = pc.getPage(li.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            //如果是撤销操作则需要把对应插入数据删除
            if(flag == UNDO) {
                //这里并非物理删除，而是逻辑删除
                DataItem.setDataItemRawInvalid(li.raw);
            }
            //执行到此处表示redo，执行插入操作
            PageX.recoverInsert(page, li.raw, li.offset);
        } finally {
          page.release();
        }
    }
}
