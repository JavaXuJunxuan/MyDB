package com.xjx.mydb.server.tm;

import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.utils.Panic;
import com.xjx.mydb.server.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2022/12/21 - 11:45
 */
public class TransactionManagerImpl implements TransactionManager {
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;
    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;
    //file和fc应该是Nio读取文件的两个类对象。file是读取之后的xid文件
    private RandomAccessFile file;
    private FileChannel fc;
    //xid文件头的事务个数
    private long xidCounter;
    //锁，用于创建事务时修改事务数
    private Lock counterLock;

    //通过传入的XID文件创建出TM，文件读写使用的是NIO的FileChannel
    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc){
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }
    //检验读取的XID文件是否合法
    private void checkXIDCounter() {
        //方式就是比较文件的理论长度和实际长度
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException ioException){
            //文件读取失败，直接强制停机
            Panic.panic(Error.BadXIDFileException);
        }
        //执行到此处表示文件读取成功
        if(fileLen < XID_HEADER_LENGTH){
            //文件检验失败，直接强制停机
            Panic.panic(Error.BadXIDFileException);
        }
        //把xid的文件头的事务总数读取进字节缓冲
        //首先分配一个等于文件头大小的字节缓冲区
        ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_LENGTH);
        try {
            //Nio方式读取字节缓冲中的值
            fc.position(0);
            fc.read(buf);
        } catch (IOException ioException) {
            //IO读取失败，直接强制停机
            Panic.panic(ioException);
        }
        //因为字节缓冲区中的值我们无法直接读出来，所以使用解析器解析成long类型
        this.xidCounter = Parser.parseLong(buf.array());
        //获取最后一个事务所在地址，+1是因为还有一个0号超级事务
        long end = getXidPosition(this.xidCounter + 1);
        //如果最后一个事务的地址即文件理论长度不等于文件实际长度那么就认为这个XID文件不合法，直接强制停机，并报XID文件错误
        if(end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    // 根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid) {
        return XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    // 开始一个事务，并返回这个新增事务的XID
    // TODO: 2023/2/16 系统暂时没有实现事务功能
    @Override
    public long begin() {
        //开始事务过程需要加锁，需要保证下面过程不能出现并发问题，否则事务服务就会出错
        counterLock.lock();
        try {
            //计算当前事务XID
            long xid = xidCounter + 1;
            //更改新建事务的状态为active
            updateXID(xid, FIELD_TRAN_ACTIVE);
            //需要修改XID文件Header中的事务总数
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    //将XID加一，并更新XID Header
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buffer = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buffer);
            //向文件中写入数据之后要手动强制将数据刷入文件中，防止数据丢失
            fc.force(false);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
    }

    //更新xid事务的状态为status
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buffer = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buffer);
            fc.force(false);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
    }

    // 提交XID事务
    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 回滚XID事务
    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buffer);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        return buffer.array()[0] == status;
    }

    @Override
    public void close() {
        try {
            file.close();
            fc.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
