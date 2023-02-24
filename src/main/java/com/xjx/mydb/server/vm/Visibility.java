package com.xjx.mydb.server.vm;

import com.xjx.mydb.server.tm.TransactionManager;

/**
 * @Author: Xjx
 * @Create: 2023/1/14 - 15:10
 */
public class Visibility {

    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e){
        long xmax = e.getXmax();
        //如果是读已提交级别则无需跳过
        if(t.level == 0){
            return false;
        } else {
            //如果当前记录被删除且删除操作的事务提交并且
            //删除操作的事务在该操作之后或者删除事务处于当前事务开启时的活跃事务，都为true则需要跳过该版本
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    //判断当前记录对某一个事务是否可见
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    public static boolean readCommitted(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //如果是当前事务创建的记录并且没有被删除则可见
        if(xmin == xid && xmax == 0) return true;
        //如果不是当前事务创建的则需要判断创建该记录的事务是否被提交
        if(tm.isCommitted(xmin)) {
            //如果没有被删除则可见
            if(xmax == 0) return true;
            //不是当前事务创建的也不是当前事务删除的则需要判断删除记录的事务是否提交
            if(xmax != xid) {
                //删除事务没有提交则可见
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        //如果当前事务创建当前事务删除则该记录不可见或者不是当前事务创建但是被当前事务删除则也不可见，即当前事务删除了则一定不可见
        //不是当前事务创建也不是当前事务删除的则需要去判断一下删除记录的事务是否提交
        return  false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //如果当前事务创建的记录且没删除则可见
        if(xmin == xid && xmax == 0) return true;
        //如果创建记录的事务提交了且小于当前事务id且提交记录的事务不在当前事务的快照内（即在当前事务开始前提交）
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            //没有被删除
            if(xmax == 0) return true;
            //因为可重复读，所以只要该事务开启时这记录不是已经被删除了都可见
            if(xmax != xid) {
                //不是被当前事务删除并且没提交||当前事务之后提交删除的||或者是当前事务开启时活跃事务提交的
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return  false;
    }
}
