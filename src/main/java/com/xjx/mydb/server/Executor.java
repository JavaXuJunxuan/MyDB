package com.xjx.mydb.server;

import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.parser.Parser;
import com.xjx.mydb.server.parser.statement.*;
import com.xjx.mydb.server.tbm.BeginRes;
import com.xjx.mydb.server.tbm.TableManager;

/**
 * @Author: Xjx
 * @Create: 2023/2/15 - 13:32
 */
public class Executor {
    private long xid;
    TableManager tbm;

    //处理器调用最外层的tbm对相关SQL语句进行处理
    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public void close() {
        if(xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
        }
    }

    //核心处理方法：处理SQL语句
    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        //首先解析SQL语句的类型,先看看是不是事务命令类型
        Object stat = Parser.parse(sql);
        if(Begin.class.isInstance(stat)) {
            //开始新事务则xid为0
            if(xid != 0) {
                throw Error.NestedTransactionException;
            }
            BeginRes r = tbm.begin((Begin) stat);
            xid = r.xid;
            return r.result;
        } else if(Commit.class.isInstance(stat)) {
            //提交事务则xid一定不为0
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if(Abort.class.isInstance(stat)) {
            //丢失事务则一定不为0
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            return execute2(stat);
        }
    }

    //执行除事务之外的其他语句(表相关语句)
    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if(xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        } catch (Exception e1) {
            e = e1;
            throw e;
        } finally {
            //true表示临时的事务，即当前SQL操作不在所管理的事务中进行。
            if(tmpTransaction) {
                //如果有异常则抛弃该事务
                if(e != null) {
                    tbm.abort(xid);
                } else {
                    //没有异常则提交该事务
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}
