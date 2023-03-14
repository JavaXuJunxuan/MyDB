package com.xjx.mydb.server.tbm;

import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.dm.DataManager;
import com.xjx.mydb.server.parser.statement.*;
import com.xjx.mydb.server.utils.Parser;
import com.xjx.mydb.server.vm.VersionManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2023/2/14 - 10:55
 */
public class TableManagerImpl implements TableManager {
    //tbm基于vm来查询表和字段数据的
    VersionManager vm;
    DataManager dm;
    //对于数据库的启动文件对象的一个引用，启动文件中存储着数据库的头表id，因为数据库表是通过链表形式连接在一起的。
    private Booter booter;
    private Map<String, Table> tableCache;
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;

    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        //初始化TBM时需要将管理的表加载进来，但是表是链表形式的，因此只需要加载第一张头表即可
        loadTables();
    }

    private void loadTables() {
        long uid = firstTableUid();
        while (uid != 0) {
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    //对外提供的开启事务的API
    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead ? 1 : 0;
        //VM开启一个新事务
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }

    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    //打印目前库中所有表结构数据
    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for(Table table : tableCache.values()) {
                sb.append(table.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            if(t == null) {
                return "\n".getBytes();
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    //对外提供的建表API
    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            //新建的表已存在
            if(tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            //新建的表都是以头插法插进表的链表中
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if(!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            //返回建表成功语句
            return ("create table: " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    //对外提供的插入API
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    //对外提供的查询API
    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, read).getBytes();
    }

    //对外提供的更新API
    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update" + count).getBytes();
    }

    //对外提供的删除API
    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete" + count).getBytes();
    }
}
