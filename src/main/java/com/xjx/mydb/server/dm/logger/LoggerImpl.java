package com.xjx.mydb.server.dm.logger;

import com.google.common.primitives.Bytes;
import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.utils.Panic;
import com.xjx.mydb.server.utils.Parser;
import org.ietf.jgss.Oid;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2022/12/30 - 16:35
 */
public class LoggerImpl implements Logger {
    //日志文件的后缀名
    public static final String LOG_SUFFIX = ".log";

    //一个种子常量，用于计算日志校验和
    private static final int SEED = 13331;
    //日志起始地址（以下皆是随即数组存取方式）
    private static final int OF_SIZE = 0;
    //日志检验和数据存放地址
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    //日志数据起始地址
    private static final int OF_DATA = OF_CHECKSUM + 4;
    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;
    //当前日志指针的位置，指向的应该是单个日志的头部即记录data长度的第一个字节处=之前全部日志的长度（因为是数组访问）
    private long position;
    //初始化时记录，log操作不更新
    private long fileSize;
    //当前日志对象对应日志文件的校验和
    private int xChecksum;

    //根据传入的文件和文件访问通道返回这个文件的日志操作对象，用于读取日志文件时
    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    //多传入一个日志文件的检验和，一般用于创建日志文件时
    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.raf = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    //初始化日志对象
    void init() {
        //获取日志文件的长度
        long size = 0;
        try {
            size = raf.length();
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        int xChecksum = Parser.parseInt(raw.array());
        //获取日志文件大小和日志文件的校验和用于初始化日志对象
        this.fileSize = size;
        this.xChecksum = xChecksum;
        checkAndRemoveTail();
    }
    //检查并移除bad tail
    private void checkAndRemoveTail() {
        rewind();
        int xCheck = 0;
        while (true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        rewind();
    }

    //该方法用于计算单个日志的校验和，通过一个种子常量与每一个日志文件的字节进行计算
    private int calChecksum(int xCheck, byte[] log) {
        for(byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return  xCheck;
    }

    //Logger被实现成迭代器模式，通过 next() 方法不断地从文件中读取下一条日志
    //并将其中的 Data 解析出来并返回
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    //next方法的具体底层实现
    private byte[] internNext() {
        //如果当前日志指针位置加上这个日志的通用属性（日志data长度4字节+检验和4字节）>=文件长度则返回null即没有下一个日志了
        if(position + OF_DATA >= fileSize) {
            return null;
        }
        //执行到这里表示当前日志文件还有空闲空间即还有下一个日志
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            //通过单个日志头部的data长度信息读取这个日志文件长度
            fc.position(position);
            fc.read(tmp);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        int size = Parser.parseInt(tmp.array());
        //如果加上当前读取的下一个文件长度之后大于文件长度，那么也返回null
        if(position + size + OF_DATA > fileSize){
            return null;
        }
        //执行到这里表示日志文件中满足再读取下一个日志的条件
        //为下一个日志分配内存
        ByteBuffer buffer = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buffer);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        //将字节缓冲中读到的日志转化成字节数组
        byte[] log = buffer.array();
        //将日志中的日志数据部分取出来并计算校验和
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        //取出这个日志的检验和部分
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        //比对两个检验和，如果不一致则表示日志记录错误返回null
        if(checkSum1 != checkSum2) {
            return null;
        }
        //执行到此处表示下一个日志读取成功，改变日志偏移量并返回日志数据
        position += log.length;
        return log;
    }

    //修改日志文件的总检验和
    private void updateXChecksum(byte[] log) {
        //传入的是之前的总校验和和这次要参与运算的一个日志
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
    }

    //将一组sql操作包装成日志
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    //将一组sql操作包装成日志并写入日志文件中
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buffer = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buffer);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    //截断指定长度的当前文件
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            raf.close();
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
    }
}
