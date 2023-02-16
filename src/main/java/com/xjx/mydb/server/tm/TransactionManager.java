package com.xjx.mydb.server.tm;

import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @Author: Xjx
 * @Create: 2022/12/20 - 19:40
 */
public interface TransactionManager {
    //XID文件头长度
    public static final int XID_HEADER_LENGTH = 8;
    //超级事务，永远为commited状态
    public static final long SUPER_XID = 0;
    //XID文件的后缀名
    public static final String XID_SUFFIX = ".xid";

    // 开启一个新事务
    long begin();
    // 提交一个事务
    void commit(long xid);
    // 取消一个事务
    void abort(long xid);
    // 关闭TM
    void close();
    // 查询一个事务的状态是否是正在进行的状态
    boolean isActive(long xid);
    // 查询一个事务的状态是否是已提交
    boolean isCommitted(long xid);
    // 查询一个事务的状态是否是已取消
    boolean isAborted(long xid);

    //创建一个 xid 文件并创建 TM。参数就是要创建的文件的抽象路径
    public static TransactionManagerImpl create(String path){
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
        //因为新建操作，所以只有当path路径对应的文件不存在时才可以创建成功，否则失败
        try {
            if(!file.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        if(!file.canRead() || !file.canWrite()){
            Panic.panic(Error.FileCannotRWExcepiton);
        }
        //程序执行到此处表示创建XID文件成功且可对其正常读写，接下来我们就对这个文件进行读写操作了
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        // 写XID文件头为空，因为新建的XID没有存储任何事务
        //把一个大小等于XID头的字节数组包装进字节缓冲中
        ByteBuffer buffer = ByteBuffer.wrap(new byte[TransactionManagerImpl.XID_HEADER_LENGTH]);
        //把字节缓冲中的数据写入XID文件
        try {
            //写入文件位置为文件头
            fc.position(0);
            fc.write(buffer);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        return new TransactionManagerImpl(raf,fc);
    }

    //从一个已有的 xid 文件来创建 TM
    public static TransactionManager open(String path){
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if(!file.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if(!file.canRead() || !file.canWrite()){
            Panic.panic(Error.FileCannotRWExcepiton);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }
}
