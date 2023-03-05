package com.xjx.mydb.server.tbm;

import com.xjx.mydb.server.dm.DataManager;
import com.xjx.mydb.server.parser.statement.*;
import com.xjx.mydb.server.utils.Parser;
import com.xjx.mydb.server.vm.VersionManager;

/**
 * @Author: Xjx
 * @Create: 2023/2/12 - 10:44
 */
public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);
    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;
    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
